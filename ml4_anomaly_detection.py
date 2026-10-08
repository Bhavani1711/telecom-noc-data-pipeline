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
# 1. LOAD HOURLY ACTIVITY
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
# 2. CLEAN AND PREPARE
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

activity["grid_id"] = activity["grid_id"].astype(int)

activity = activity.sort_values(
    ["grid_id", "timestamp"]
).reset_index(drop=True)

activity["hour"] = activity["timestamp"].dt.hour


# ============================================================
# 3. HISTORICAL GRID + HOUR-OF-DAY BASELINE
# ============================================================
#
# For every grid/hour combination:
#
# Example:
# Grid 4857, hour 14
#
# Day 1 -> no history
# Day 2 -> 1 historical observation
# Day 3 -> 2 historical observations
# Day 4 -> median of Days 1-3
# Day 5 -> median of Days 1-4
# ...
#
# IMPORTANT:
# Current and future observations are NEVER used.
#
# This is the key leakage-control requirement for ML4.
# ============================================================

group_cols = ["grid_id", "hour"]

grouped_activity = activity.groupby(
    group_cols,
    sort=False,
)

activity["history_count"] = grouped_activity[
    "total_activity"
].transform(
    lambda s: s.shift(1).expanding().count()
)

activity["baseline_activity"] = grouped_activity[
    "total_activity"
].transform(
    lambda s: s.shift(1).expanding().median()
)


# ============================================================
# 4. HISTORICAL MAD
# ============================================================
#
# MAD = median absolute deviation.
#
# It provides a robust measure of historical variability.
#
# We calculate it using ONLY historical observations.
# ============================================================

def historical_mad(series):

    values = series.to_numpy(dtype=float)

    output = np.full(
        len(values),
        np.nan,
        dtype=float,
    )

    for i in range(len(values)):

        historical = values[:i]

        if len(historical) < MIN_HISTORY:
            continue

        median_value = np.median(
            historical
        )

        output[i] = np.median(
            np.abs(
                historical - median_value
            )
        )

    return pd.Series(
        output,
        index=series.index,
    )


activity["historical_mad"] = grouped_activity[
    "total_activity"
].transform(
    historical_mad
)


# ============================================================
# 5. PERCENTAGE DEVIATION
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


# ============================================================
# 6. ROBUST ANOMALY SCORE
# ============================================================
#
# Robust z-score:
#
# (current - historical median)
# --------------------------------
#       1.4826 * historical MAD
#
# The score is directional:
# positive = unusually high
# negative = unusually low
# ============================================================

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
# 7. ANOMALY CLASSIFICATION
# ============================================================

activity["anomaly_direction"] = "NORMAL"

high_condition = (
    (activity["history_count"] >= MIN_HISTORY)
    & (
        activity["percentage_deviation"]
        >= HIGH_DEVIATION_THRESHOLD
    )
    & (
        activity["anomaly_score"]
        >= MAD_Z_THRESHOLD
    )
)

low_condition = (
    (activity["history_count"] >= MIN_HISTORY)
    & (
        activity["percentage_deviation"]
        <= LOW_DEVIATION_THRESHOLD
    )
    & (
        activity["anomaly_score"]
        <= -MAD_Z_THRESHOLD
    )
)

activity.loc[
    high_condition,
    "anomaly_direction",
] = "HIGH_ANOMALY"

activity.loc[
    low_condition,
    "anomaly_direction",
] = "LOW_ANOMALY"


# ============================================================
# 8. HUMAN-READABLE REASON
# ============================================================

def build_reason(row):

    if row["history_count"] < MIN_HISTORY:

        return (
            "Insufficient historical observations for "
            "a reliable same-grid same-hour baseline."
        )

    if pd.isna(row["baseline_activity"]):

        return (
            "Historical baseline is unavailable."
        )

    deviation = row["percentage_deviation"]
    score = row["anomaly_score"]

    if row["anomaly_direction"] == "HIGH_ANOMALY":

        return (
            f"Activity is {deviation * 100:.1f}% above "
            f"the historical median for grid "
            f"{int(row['grid_id'])} at hour "
            f"{int(row['hour']):02d}:00; "
            f"robust anomaly score is {score:.2f}."
        )

    if row["anomaly_direction"] == "LOW_ANOMALY":

        return (
            f"Activity is {abs(deviation) * 100:.1f}% below "
            f"the historical median for grid "
            f"{int(row['grid_id'])} at hour "
            f"{int(row['hour']):02d}:00; "
            f"robust anomaly score is {score:.2f}."
        )

    return (
        f"Activity is within the configured historical "
        f"range for grid {int(row['grid_id'])} at hour "
        f"{int(row['hour']):02d}:00."
    )


activity["reason"] = activity.apply(
    build_reason,
    axis=1,
)


# ============================================================
# 9. NP3 ALERT COMPARISON
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
            suffixes=("", "_np3"),
        )

        activity["np3_alert"] = (
            activity["np3_alert_np3"]
            .fillna(0)
            .astype(int)
        )

        activity = activity.drop(
            columns=["np3_alert_np3"]
        )


# ============================================================
# 10. ML3 COMPARISON
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
# 11. THREE-WAY COMPARISON
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
# 12. OUTPUT TABLE
# ============================================================

output_columns = [
    "grid_id",
    "timestamp",
    "hour",
    "total_activity",
    "history_count",
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
# 13. SAVE CSV
# ============================================================

result.to_csv(
    OUTPUT_FILE,
    index=False,
)


# ============================================================
# 14. SAVE SQLITE TABLE
# ============================================================

with sqlite3.connect(DB_PATH) as conn:

    result.to_sql(
        "network_anomaly_scores",
        conn,
        if_exists="replace",
        index=False,
    )


# ============================================================
# 15. SUMMARY
# ============================================================

usable_baseline = result[
    result["history_count"] >= MIN_HISTORY
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

ml4_count = int(
    result["ml4_flag"].sum()
)

ml3_count = int(
    result["ml3_prediction"].sum()
)

np3_count = int(
    result["np3_alert"].sum()
)

comparison_counts = (
    result["comparison"]
    .value_counts()
    .to_dict()
)


# ============================================================
# 16. REPORT
# ============================================================

first_timestamp = result["timestamp"].min()
last_timestamp = result["timestamp"].max()

report_lines = [
    "# ML4 — Historical Anomaly Baseline",
    "",
    "## Objective",
    "",
    "ML4 provides an independent anomaly mechanism based on "
    "historical activity for the same grid and hour-of-day.",
    "",
    "Unlike NP3's within-day comparison, ML4 uses accumulated "
    "history and therefore provides cross-day hour-of-day context.",
    "",
    "## Baseline definition",
    "",
    "- Grouping: `grid_id + hour-of-day`",
    "- Baseline statistic: historical median",
    f"- Minimum historical observations: {MIN_HISTORY}",
    "- Current observation excluded",
    "- Future observations excluded",
    "",
    "Every baseline uses only observations strictly earlier "
    "than the current timestamp, preventing future leakage.",
    "",
    "## Anomaly scoring",
    "",
    f"- High deviation threshold: +{HIGH_DEVIATION_THRESHOLD:.0%}",
    f"- Low deviation threshold: {LOW_DEVIATION_THRESHOLD:.0%}",
    f"- Robust z-score threshold: ±{MAD_Z_THRESHOLD:.1f}",
    "- Robust score uses historical median absolute deviation (MAD).",
    "",
    "## Dataset coverage",
    "",
    f"- Activity range: **{first_timestamp} → {last_timestamp}**",
    f"- Total grid/hour observations: **{len(result):,}**",
    f"- Usable historical observations: **{len(usable_baseline):,}**",
    f"- Insufficient-history observations: "
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
    f"- ML3 high-risk predictions: **{ml3_count:,}**",
    f"- ML4 anomaly flags: **{ml4_count:,}**",
    f"- NP3 rule alerts: **{np3_count:,}**",
    "",
    "### NP3",
    "",
    "NP3 compares current activity with the grid's other "
    "hours within the same day. It is useful for within-day "
    "deviation detection but does not learn cross-day patterns.",
    "",
    "### ML3",
    "",
    "ML3 predicts next-hour high-activity risk using engineered "
    "features. It is predictive rather than a direct anomaly detector.",
    "",
    "### ML4",
    "",
    "ML4 asks whether current activity is unusual compared with "
    "historical observations for the same grid and hour-of-day.",
    "",
    "## Three operational observations",
    "",
    "1. **ML4 adds historical context.** "
    "A grid may look normal relative to other hours of the same "
    "day but still be unusual compared with its historical "
    "behavior at that hour.",
    "",
    "2. **ML3 + ML4 agreement is a useful investigation signal.** "
    "When the predictive model and independent historical baseline "
    "both flag a grid, the case can receive higher investigation priority.",
    "",
    "3. **Disagreement is expected and informative.** "
    "ML3 predicts future high activity from engineered features, "
    "while ML4 evaluates current behavior against historical patterns. "
    "NP3 uses a separate within-day rule. They answer different questions.",
    "",
    "## Limitations",
    "",
    "- Approximately seven days of history are currently available; "
    "14+ days would provide a stronger historical baseline.",
    "- Each grid/hour-of-day combination therefore has limited history.",
    "- Anomaly detection identifies unusual behavior but not its cause.",
    "- An anomaly is not proof of congestion, failure, or customer impact.",
    "- Results should be treated as investigation signals.",
    "",
    "## Artifacts",
    "",
    f"- `{OUTPUT_FILE}`",
    f"- `{REPORT_FILE}`",
    "- SQLite table: `network_anomaly_scores`",
]

REPORT_FILE.write_text(
    "\n".join(report_lines)
)


# ============================================================
# 17. FINAL CONSOLE OUTPUT
# ============================================================

print()
print("=" * 60)
print("ML4 ANOMALY BASELINE COMPLETE")
print("=" * 60)

print(
    f"Activity rows:              {len(result):,}"
)

print(
    f"Usable historical rows:     {len(usable_baseline):,}"
)

print()
print(
    f"HIGH_ANOMALY:               {high_count:,}"
)

print(
    f"LOW_ANOMALY:                {low_count:,}"
)

print(
    f"NORMAL:                     {normal_count:,}"
)

print()
print(
    f"ML3 flags:                  {ml3_count:,}"
)

print(
    f"ML4 flags:                  {ml4_count:,}"
)

print(
    f"NP3 alerts:                 {np3_count:,}"
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
