import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from baseline_utils import calculate_excluding_current_baseline


# ============================================================
# CONFIGURATION
# ============================================================

DB_PATH = Path("data/network_analytics.db")

OUTPUT_FILE = Path("network_anomaly_scores.csv")
REPORT_FILE = Path("ML4_Anomaly_Report.md")

NP3_ALERT_FILE = Path("network_alerts.csv")
ML3_PREDICTION_FILE = Path("ml3_predictions.csv")

MIN_HISTORY = 3
HIGH_DEVIATION_THRESHOLD = 0.50
LOW_DEVIATION_THRESHOLD = -0.50
MAD_Z_THRESHOLD = 3.0


# ============================================================
# 1. LOAD DATA
# ============================================================

if not DB_PATH.exists():
    raise FileNotFoundError(
        f"Database not found: {DB_PATH}"
    )

with sqlite3.connect(DB_PATH) as conn:

    activity = pd.read_sql_query(
        """
        SELECT
            grid_id,
            timestamp,
            total_activity
        FROM hourly_grid_summary
        ORDER BY timestamp, grid_id
        """,
        conn,
    )


if activity.empty:
    raise ValueError(
        "hourly_grid_summary contains no rows."
    )


# ============================================================
# 2. PREPARE DATA
# ============================================================

activity["timestamp"] = pd.to_datetime(
    activity["timestamp"],
    errors="coerce",
)

activity["total_activity"] = pd.to_numeric(
    activity["total_activity"],
    errors="coerce",
)

activity = activity.dropna(
    subset=[
        "grid_id",
        "timestamp",
        "total_activity",
    ]
).copy()

activity = activity.sort_values(
    ["grid_id", "timestamp"]
).reset_index(drop=True)

activity["hour"] = (
    activity["timestamp"]
    .dt.hour
)


# ============================================================
# 3. CALCULATE HISTORICAL HOUR-OF-DAY BASELINE
# ============================================================

# ML4 compares each observation with historical observations
# from the same grid and hour-of-day.
#
# Only observations STRICTLY BEFORE the current timestamp are
# allowed to contribute to the historical baseline.
#
# We use the reusable NP3 baseline implementation first to
# establish the same grouping/exclusion mechanism.
#
# The historical filtering below prevents future observations
# from entering the ML4 baseline.

activity["baseline_activity"] = np.nan

for (grid_id, hour), group in activity.groupby(
    ["grid_id", "hour"],
    sort=False,
):

    group = group.sort_values(
        "timestamp"
    )

    values = group["total_activity"].to_numpy(
        dtype=float
    )

    timestamps = group["timestamp"].to_numpy()

    indices = group.index.to_numpy()

    for position, index in enumerate(indices):

        historical_values = values[
            timestamps < timestamps[position]
        ]

        if len(historical_values) >= MIN_HISTORY:

            # Reuse the same median-baseline definition.
            # The helper's exclusion rule means the current
            # observation cannot contribute to the baseline.
            historical_frame = pd.DataFrame(
                {
                    "grid_id": [grid_id] * len(historical_values),
                    "hour": [hour] * len(historical_values),
                    "total_activity": historical_values,
                }
            )

            baseline = calculate_excluding_current_baseline(
                historical_frame,
                bucket_columns=["grid_id", "hour"],
                value_column="total_activity",
            )

            activity.loc[
                index,
                "baseline_activity"
            ] = baseline.iloc[-1]


# ============================================================
# 4. HISTORICAL ROBUST VARIABILITY
# ============================================================

# A median absolute deviation (MAD) baseline is used so that
# unusually large historical values do not dominate the
# variability estimate.

activity["historical_mad"] = np.nan

for (grid_id, hour), group in activity.groupby(
    ["grid_id", "hour"],
    sort=False,
):

    group = group.sort_values(
        "timestamp"
    )

    values = group["total_activity"].to_numpy(
        dtype=float
    )

    timestamps = group["timestamp"].to_numpy()

    indices = group.index.to_numpy()

    for position, index in enumerate(indices):

        historical_values = values[
            timestamps < timestamps[position]
        ]

        if len(historical_values) >= MIN_HISTORY:

            median_value = np.median(
                historical_values
            )

            mad = np.median(
                np.abs(
                    historical_values
                    - median_value
                )
            )

            activity.loc[
                index,
                "historical_mad"
            ] = mad


# ============================================================
# 5. DEVIATION AND ANOMALY SCORE
# ============================================================

activity["percentage_deviation"] = np.where(
    activity["baseline_activity"] > 0,
    (
        activity["total_activity"]
        - activity["baseline_activity"]
    )
    / activity["baseline_activity"],
    np.nan,
)

# Robust z-score:
#
# z = (current - median) / (1.4826 * MAD)
#
# The constant converts MAD approximately to a standard
# deviation scale under a normal distribution.

activity["anomaly_score"] = np.where(
    activity["historical_mad"] > 0,
    (
        activity["total_activity"]
        - activity["baseline_activity"]
    )
    / (
        1.4826
        * activity["historical_mad"]
    ),
    np.nan,
)


# ============================================================
# 6. CLASSIFY ANOMALY DIRECTION
# ============================================================

activity["anomaly_direction"] = "NORMAL"

high_condition = (
    activity["percentage_deviation"]
    >= HIGH_DEVIATION_THRESHOLD
) & (
    activity["anomaly_score"]
    >= MAD_Z_THRESHOLD
)

low_condition = (
    activity["percentage_deviation"]
    <= LOW_DEVIATION_THRESHOLD
) & (
    activity["anomaly_score"]
    <= -MAD_Z_THRESHOLD
)

activity.loc[
    high_condition,
    "anomaly_direction"
] = "HIGH_ANOMALY"

activity.loc[
    low_condition,
    "anomaly_direction"
] = "LOW_ANOMALY"


# ============================================================
# 7. HUMAN-READABLE REASON
# ============================================================

def build_reason(row):

    if pd.isna(row["baseline_activity"]):

        return (
            "Insufficient historical observations for "
            "a reliable same-grid same-hour baseline."
        )

    deviation = row["percentage_deviation"]
    score = row["anomaly_score"]

    if row["anomaly_direction"] == "HIGH_ANOMALY":

        return (
            f"Activity is {deviation * 100:.1f}% above "
            f"the historical median for grid {int(row['grid_id'])} "
            f"at hour {int(row['hour']):02d}:00; "
            f"robust anomaly score is {score:.2f}."
        )

    if row["anomaly_direction"] == "LOW_ANOMALY":

        return (
            f"Activity is {abs(deviation) * 100:.1f}% below "
            f"the historical median for grid {int(row['grid_id'])} "
            f"at hour {int(row['hour']):02d}:00; "
            f"robust anomaly score is {score:.2f}."
        )

    return (
        f"Activity is within the configured historical "
        f"anomaly range for grid {int(row['grid_id'])} "
        f"at hour {int(row['hour']):02d}:00."
    )


activity["reason"] = activity.apply(
    build_reason,
    axis=1,
)


# ============================================================
# 8. NP3 COMPARISON
# ============================================================

activity["np3_alert"] = 0

if NP3_ALERT_FILE.exists():

    np3 = pd.read_csv(
        NP3_ALERT_FILE
    )

    if not np3.empty:

        np3["timestamp"] = pd.to_datetime(
            np3["timestamp"],
            errors="coerce",
        )

        np3_keys = (
            np3[
                ["grid_id", "timestamp"]
            ]
            .dropna()
            .drop_duplicates()
        )

        np3_keys["np3_alert"] = 1

        activity = activity.merge(
            np3_keys,
            on=["grid_id", "timestamp"],
            how="left",
            suffixes=("", "_from_np3"),
        )

        activity["np3_alert"] = (
            activity["np3_alert_from_np3"]
            .fillna(0)
            .astype(int)
        )

        activity = activity.drop(
            columns=["np3_alert_from_np3"]
        )


# ============================================================
# 9. ML3 COMPARISON
# ============================================================

activity["ml3_prediction"] = 0

if ML3_PREDICTION_FILE.exists():

    ml3 = pd.read_csv(
        ML3_PREDICTION_FILE
    )

    if not ml3.empty:

        ml3["target_timestamp"] = pd.to_datetime(
            ml3["target_timestamp"],
            errors="coerce",
        )

        ml3_keys = (
            ml3[
                [
                    "grid_id",
                    "target_timestamp",
                    "model_prediction",
                ]
            ]
            .dropna(
                subset=[
                    "grid_id",
                    "target_timestamp",
                ]
            )
            .drop_duplicates(
                subset=[
                    "grid_id",
                    "target_timestamp",
                ]
            )
        )

        activity = activity.merge(
            ml3_keys,
            left_on=[
                "grid_id",
                "timestamp",
            ],
            right_on=[
                "grid_id",
                "target_timestamp",
            ],
            how="left",
        )

        activity["ml3_prediction"] = (
            activity["model_prediction"]
            .fillna(0)
            .astype(int)
        )

        activity = activity.drop(
            columns=[
                "target_timestamp",
                "model_prediction",
            ]
        )


# ============================================================
# 10. THREE-WAY COMPARISON
# ============================================================

activity["ml4_flag"] = (
    activity["anomaly_direction"]
    != "NORMAL"
).astype(int)

activity["comparison"] = np.select(
    [
        (
            (activity["ml4_flag"] == 1)
            & (activity["ml3_prediction"] == 1)
            & (activity["np3_alert"] == 1)
        ),

        (
            (activity["ml4_flag"] == 1)
            & (activity["ml3_prediction"] == 1)
            & (activity["np3_alert"] == 0)
        ),

        (
            (activity["ml4_flag"] == 1)
            & (activity["ml3_prediction"] == 0)
            & (activity["np3_alert"] == 1)
        ),

        (
            (activity["ml4_flag"] == 1)
            & (activity["ml3_prediction"] == 0)
            & (activity["np3_alert"] == 0)
        ),

        (
            (activity["ml4_flag"] == 0)
            & (activity["ml3_prediction"] == 1)
            & (activity["np3_alert"] == 0)
        ),

        (
            (activity["ml4_flag"] == 0)
            & (activity["ml3_prediction"] == 0)
            & (activity["np3_alert"] == 1)
        ),
    ],
    [
        "ALL_THREE",
        "ML3_ML4",
        "NP3_ML4",
        "ML4_ONLY",
        "ML3_ONLY",
        "NP3_ONLY",
    ],
    default="NONE",
)


# ============================================================
# 11. SELECT OUTPUT
# ============================================================

output_columns = [
    "grid_id",
    "timestamp",
    "hour",
    "total_activity",
    "baseline_activity",
    "historical_mad",
    "percentage_deviation",
    "anomaly_score",
    "anomaly_direction",
    "ml4_flag",
    "ml3_prediction",
    "np3_alert",
    "comparison",
    "reason",
]

result = activity[
    output_columns
].copy()


# ============================================================
# 12. SAVE CSV
# ============================================================

result.to_csv(
    OUTPUT_FILE,
    index=False,
)


# ============================================================
# 13. SAVE DATABASE TABLE
# ============================================================

with sqlite3.connect(DB_PATH) as conn:

    result.to_sql(
        "network_anomaly_scores",
        conn,
        if_exists="replace",
        index=False,
    )


# ============================================================
# 14. SUMMARY STATISTICS
# ============================================================

usable_baseline = result[
    result["baseline_activity"].notna()
]

high_count = (
    result["anomaly_direction"]
    == "HIGH_ANOMALY"
).sum()

low_count = (
    result["anomaly_direction"]
    == "LOW_ANOMALY"
).sum()

normal_count = (
    result["anomaly_direction"]
    == "NORMAL"
).sum()

ml4_count = result["ml4_flag"].sum()
ml3_count = result["ml3_prediction"].sum()
np3_count = result["np3_alert"].sum()

comparison_counts = (
    result["comparison"]
    .value_counts()
    .to_dict()
)


# ============================================================
# 15. GENERATE REPORT
# ============================================================

first_timestamp = result["timestamp"].min()
last_timestamp = result["timestamp"].max()

history_observations = (
    result["baseline_activity"]
    .notna()
    .sum()
)

report_lines = [
    "# ML4 — Historical Anomaly Baseline",
    "",
    "## Objective",
    "",
    "ML4 provides an independent anomaly mechanism based on "
    "historical activity for the same grid and hour-of-day.",
    "",
    "Unlike NP3's within-day comparison, ML4 uses accumulated "
    "history and therefore provides a cross-day hour-of-day baseline.",
    "",
    "## Baseline definition",
    "",
    "- Grouping: `grid_id + hour-of-day`",
    "- Baseline statistic: historical median",
    f"- Minimum historical observations: {MIN_HISTORY}",
    "- Current observation is excluded",
    "- Future observations are excluded",
    "",
    "The baseline is therefore calculated using only observations "
    "strictly earlier than the current timestamp.",
    "",
    "## Anomaly scoring",
    "",
    f"- High deviation threshold: +{HIGH_DEVIATION_THRESHOLD:.0%}",
    f"- Low deviation threshold: {LOW_DEVIATION_THRESHOLD:.0%}",
    f"- Robust z-score threshold: ±{MAD_Z_THRESHOLD:.1f}",
    "- Robust score uses median absolute deviation (MAD).",
    "",
    "## Dataset coverage",
    "",
    f"- Activity range: **{first_timestamp} → {last_timestamp}**",
    f"- Total grid/hour observations: **{len(result):,}**",
    f"- Observations with usable historical baseline: **{len(usable_baseline):,}**",
    f"- Observations without sufficient history: "
    f"**{len(result) - len(usable_baseline):,}**",
    "",
    "## ML4 results",
    "",
    f"- HIGH_ANOMALY: **{high_count:,}**",
    f"- LOW_ANOMALY: **{low_count:,}**",
    f"- NORMAL: **{normal_count:,}**",
    f"- Total ML4 flags: **{ml4_count:,}**",
    "",
    "## Three-way comparison",
    "",
    f"- ALL_THREE: **{comparison_counts.get('ALL_THREE', 0):,}**",
    f"- ML3_ML4: **{comparison_counts.get('ML3_ML4', 0):,}**",
    f"- NP3_ML4: **{comparison_counts.get('NP3_ML4', 0):,}**",
    f"- ML4_ONLY: **{comparison_counts.get('ML4_ONLY', 0):,}**",
    f"- ML3_ONLY: **{comparison_counts.get('ML3_ONLY', 0):,}**",
    f"- NP3_ONLY: **{comparison_counts.get('NP3_ONLY', 0):,}**",
    f"- NONE: **{comparison_counts.get('NONE', 0):,}**",
    "",
    "## Mechanism comparison",
    "",
    f"- ML3 predicted high risk: **{ml3_count:,}**",
    f"- ML4 flagged an anomaly: **{ml4_count:,}**",
    f"- NP3 generated alerts: **{np3_count:,}**",
    "",
    "### NP3",
    "",
    "NP3 compares the current hour with other available hours "
    "within the same grid and day. It is useful for detecting "
    "within-day deviations but does not learn cross-day patterns.",
    "",
    "### ML3",
    "",
    "ML3 predicts next-hour high-activity risk from engineered "
    "features. It is predictive rather than a direct anomaly score.",
    "",
    "### ML4",
    "",
    "ML4 independently asks whether the current observation is "
    "unusual compared with historical observations for the same "
    "grid and hour-of-day.",
    "",
    "## Interpretation",
    "",
    "1. **ML4 adds historical context.** "
    "A grid can be normal relative to other hours of the same day "
    "but unusual compared with its historical pattern at that hour.",
    "",
    "2. **Agreement between ML3 and ML4 is operationally interesting.** "
    "When the predictive model and independent historical baseline "
    "both flag a grid, the case deserves stronger investigation priority.",
    "",
    "3. **Disagreement is informative rather than an error.** "
    "ML3 predicts future high activity using feature relationships, "
    "while ML4 evaluates the current observation against historical "
    "behavior. NP3 uses a third, within-day rule. These mechanisms "
    "answer different questions.",
    "",
    "## Important limitations",
    "",
    "- Only about seven days of accumulated history are currently "
    "available; a longer history would make hour-of-day baselines "
    "more reliable.",
    "- The current dataset provides limited observations for each "
    "grid/hour-of-day combination.",
    "- The anomaly score indicates unusual behavior, not the physical "
    "cause of the behavior.",
    "- Anomaly flags are investigation signals, not proof of congestion, "
    "failure, or customer-impacting incidents.",
    "",
    "## Artifacts",
    "",
    f"- CSV: `{OUTPUT_FILE}`",
    "- Database table: `network_anomaly_scores`",
    f"- Report: `{REPORT_FILE}`",
]


REPORT_FILE.write_text(
    "\n".join(report_lines)
)


# ============================================================
# 16. CONSOLE SUMMARY
# ============================================================

print()
print("=" * 60)
print("ML4 ANOMALY BASELINE COMPLETE")
print("=" * 60)

print(
    f"Activity rows:             {len(result):,}"
)

print(
    f"Usable historical baselines:{len(usable_baseline):,}"
)

print()
print(
    f"HIGH_ANOMALY:              {high_count:,}"
)

print(
    f"LOW_ANOMALY:               {low_count:,}"
)

print(
    f"NORMAL:                    {normal_count:,}"
)

print()
print(
    f"ML3 flags:                 {ml3_count:,}"
)

print(
    f"ML4 flags:                 {ml4_count:,}"
)

print(
    f"NP3 alerts:                {np3_count:,}"
)

print()
print("Three-way comparison:")

for label in [
    "ALL_THREE",
    "ML3_ML4",
    "NP3_ML4",
    "ML4_ONLY",
    "ML3_ONLY",
    "NP3_ONLY",
    "NONE",
]:

    print(
        f"  {label:<12} "
        f"{comparison_counts.get(label, 0):,}"
    )

print()
print("Generated:")
print(f"- {OUTPUT_FILE}")
print(f"- {REPORT_FILE}")
print("- database table: network_anomaly_scores")

print()
print("PROCESS COMPLETE")
