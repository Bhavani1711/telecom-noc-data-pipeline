import pandas as pd
import numpy as np
import logging
from pathlib import Path


# ============================================================
# CONFIGURATION
# ============================================================

INPUT_FILE = "grid_hourly_analytics.csv"
OUTPUT_FILE = "network_alerts.csv"
SUMMARY_FILE = "rule_based_alert_summary.csv"
LOG_FILE = "network_alerts.log"

HIGH_MULTIPLIER = 1.5
DROP_MULTIPLIER = 0.5
SPIKE_MULTIPLIER = 1.5


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# 1. LOAD GRID/HOUR ANALYTICS
# ============================================================

df = pd.read_csv(INPUT_FILE)

required_columns = [
    "timestamp",
    "grid_id",
    "total_activity"
]

missing = [
    col for col in required_columns
    if col not in df.columns
]

if missing:
    raise ValueError(
        f"Missing required columns: {missing}"
    )

df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    errors="coerce"
)

df["total_activity"] = pd.to_numeric(
    df["total_activity"],
    errors="coerce"
)

before_clean = len(df)

df = df.dropna(
    subset=[
        "timestamp",
        "grid_id",
        "total_activity"
    ]
).copy()

logger.info(
    "Loaded %d records",
    before_clean
)

logger.info(
    "Dropped %d rows with missing required values",
    before_clean - len(df)
)


# ============================================================
# 2. CREATE DATE AND SORT
# ============================================================

df["date"] = df["timestamp"].dt.date

df = df.sort_values(
    ["grid_id", "timestamp"]
).reset_index(drop=True)


# ============================================================
# 3. CALCULATE DAILY TOTALS
# ============================================================

daily_totals = (
    df.groupby(
        ["grid_id", "date"]
    )["total_activity"]
    .sum()
    .reset_index(name="daily_total_activity")
)


# ============================================================
# 4. ACTIVITY FLOOR
# ============================================================

# The floor is selected from the actual data.
# We use the 10th percentile of daily grid activity.
#
# Grid-days below this value are excluded because very
# low-volume grids can produce unstable ratios and false alerts.

activity_floor = daily_totals[
    "daily_total_activity"
].quantile(0.10)

print(
    f"Activity floor: {activity_floor:.2f}"
)

print(
    "Reason: grid-days below the 10th percentile are excluded "
    "because very low-volume grids can produce unstable ratios "
    "and disproportionately increase false alerts."
)

logger.info(
    "Activity floor = %.2f (10th percentile of daily grid totals)",
    activity_floor
)


# ============================================================
# 5. IDENTIFY ELIGIBLE GRID-DAYS
# ============================================================

eligible_days = daily_totals[
    daily_totals["daily_total_activity"] >= activity_floor
][
    ["grid_id", "date"]
]

logger.info(
    "Eligible grid-days: %d of %d",
    len(eligible_days),
    len(daily_totals)
)


# ============================================================
# 6. KEEP ALL HOURS FOR ELIGIBLE GRID-DAYS
# ============================================================

eligible = df.merge(
    eligible_days,
    on=["grid_id", "date"],
    how="inner"
).copy()

logger.info(
    "Eligible grid/hour records: %d",
    len(eligible)
)


# ============================================================
# 7. WITHIN-DAY BASELINE
# ============================================================

# For every grid/day:
# baseline = median of all other available hourly
# activity values, excluding the current hour.

eligible["baseline_activity"] = np.nan

for (grid, date), group in eligible.groupby(
    ["grid_id", "date"]
):

    values = group["total_activity"].to_numpy()

    for position, index in enumerate(group.index):

        other_values = np.delete(
            values,
            position
        )

        if len(other_values) > 0:

            eligible.loc[
                index,
                "baseline_activity"
            ] = np.median(other_values)


# ============================================================
# 8. PREVIOUS IMMEDIATELY PRECEDING HOUR
# ============================================================

eligible["previous_activity"] = (
    eligible.groupby("grid_id")["total_activity"]
    .shift(1)
)

eligible["previous_timestamp"] = (
    eligible.groupby("grid_id")["timestamp"]
    .shift(1)
)

# Only keep the previous value if it is exactly one hour earlier.

hour_difference = (
    eligible["timestamp"]
    - eligible["previous_timestamp"]
)

eligible.loc[
    hour_difference != pd.Timedelta(hours=1),
    "previous_activity"
] = np.nan


# ============================================================
# 9. APPLY ALERT RULES
# ============================================================

alerts = []

for _, row in eligible.iterrows():

    current = row["total_activity"]
    baseline = row["baseline_activity"]
    previous = row["previous_activity"]

    grid = row["grid_id"]
    timestamp = row["timestamp"]

    # Cannot calculate a meaningful ratio without a baseline.
    if pd.isna(baseline) or baseline <= 0:
        continue


    # --------------------------------------------------------
    # RULE 1: HIGH_ACTIVITY
    # --------------------------------------------------------

    if current >= HIGH_MULTIPLIER * baseline:

        alerts.append({
            "grid_id": grid,
            "timestamp": timestamp,
            "alert_type": "HIGH_ACTIVITY",
            "current_activity": current,
            "baseline_activity": baseline,
            "reason": (
                f"Current activity is "
                f"{current / baseline:.2f}x "
                f"the within-day baseline."
            )
        })


    # --------------------------------------------------------
    # RULE 2: ACTIVITY_DROP
    # --------------------------------------------------------

    if current <= DROP_MULTIPLIER * baseline:

        alerts.append({
            "grid_id": grid,
            "timestamp": timestamp,
            "alert_type": "ACTIVITY_DROP",
            "current_activity": current,
            "baseline_activity": baseline,
            "reason": (
                f"Current activity is "
                f"{current / baseline:.2f}x "
                f"the within-day baseline."
            )
        })


    # --------------------------------------------------------
    # RULE 3: ACTIVITY_SPIKE
    # --------------------------------------------------------

    if (
        pd.notna(previous)
        and previous > 0
        and current >= SPIKE_MULTIPLIER * previous
    ):

        alerts.append({
            "grid_id": grid,
            "timestamp": timestamp,
            "alert_type": "ACTIVITY_SPIKE",
            "current_activity": current,
            "baseline_activity": baseline,
            "reason": (
                f"Current activity increased to "
                f"{current / previous:.2f}x "
                f"the immediately preceding hour."
            )
        })


# ============================================================
# 10. CREATE ALERT DATAFRAME
# ============================================================

alerts_df = pd.DataFrame(
    alerts,
    columns=[
        "grid_id",
        "timestamp",
        "alert_type",
        "current_activity",
        "baseline_activity",
        "reason"
    ]
)


# ============================================================
# 11. EXPORT ALERTS
# ============================================================

alerts_df.to_csv(
    OUTPUT_FILE,
    index=False
)

logger.info(
    "Exported %d alerts to %s",
    len(alerts_df),
    Path(OUTPUT_FILE).resolve()
)


# ============================================================
# 12. OPERATIONAL SUMMARY
# ============================================================

total_grid_hours = len(eligible)

total_alerts = len(alerts_df)

if total_grid_hours > 0:

    alert_proportion = (
        total_alerts / total_grid_hours
    ) * 100

else:

    alert_proportion = 0


print("\n========== NETWORK ALERT SUMMARY ==========")

print(
    f"\nTotal grid/hour records: {len(df)}"
)

print(
    f"Eligible grid/hour records: {len(eligible)}"
)

print(
    f"Activity floor: {activity_floor:.2f}"
)

print(
    f"Total alerts: {total_alerts}"
)


# ============================================================
# 13. ALERTS BY TYPE
# ============================================================

print("\nAlerts by type:")

if total_alerts > 0:

    alerts_by_type = (
        alerts_df["alert_type"]
        .value_counts()
    )

    print(alerts_by_type)

else:

    alerts_by_type = pd.Series(
        {
            "HIGH_ACTIVITY": 0,
            "ACTIVITY_SPIKE": 0,
            "ACTIVITY_DROP": 0
        }
    )

    print(alerts_by_type)


# ============================================================
# 14. TOP 10 GRIDS
# ============================================================

print("\nTop 10 grids by alert count:")

if total_alerts > 0:

    top_grids = (
        alerts_df["grid_id"]
        .value_counts()
        .head(10)
    )

    print(top_grids)

else:

    top_grids = pd.Series(dtype="int64")

    print("No grids generated alerts.")


# ============================================================
# 15. ALERT PROPORTION
# ============================================================

print(
    f"\nProportion of all eligible grid/hours that alerted: "
    f"{alert_proportion:.2f}%"
)


# ============================================================
# 16. EXPORT RULE-BASED SUMMARY
# ============================================================

summary_rows = [
    {
        "metric": "total_grid_hours",
        "value": total_grid_hours
    },
    {
        "metric": "total_alerts",
        "value": total_alerts
    },
    {
        "metric": "alert_proportion_percent",
        "value": round(alert_proportion, 2)
    },
    {
        "metric": "high_activity_alerts",
        "value": int(
            alerts_by_type.get(
                "HIGH_ACTIVITY",
                0
            )
        )
    },
    {
        "metric": "activity_spike_alerts",
        "value": int(
            alerts_by_type.get(
                "ACTIVITY_SPIKE",
                0
            )
        )
    },
    {
        "metric": "activity_drop_alerts",
        "value": int(
            alerts_by_type.get(
                "ACTIVITY_DROP",
                0
            )
        )
    },
    {
        "metric": "activity_floor",
        "value": round(activity_floor, 2)
    }
]

summary_df = pd.DataFrame(summary_rows)

summary_df.to_csv(
    SUMMARY_FILE,
    index=False
)

logger.info(
    "Rule-based summary exported to %s",
    Path(SUMMARY_FILE).resolve()
)


# ============================================================
# 17. LIMITATIONS
# ============================================================

limitations = (
    "The within-day baseline detects unusual activity relative "
    "to a grid's own activity during the same day. It cannot "
    "distinguish whether an alert is caused by network failure, "
    "planned maintenance, a public event, congestion, equipment "
    "problems, abnormal customer behavior, or a genuine incident. "
    "It also does not learn longer-term historical, weekly, "
    "seasonal, or holiday patterns. Therefore, an alert is a "
    "request for investigation and not a diagnosis."
)

print("\nBaseline limitations:")
print(limitations)

logger.info(
    "Baseline limitations: %s",
    limitations
)


# ============================================================
# 18. FINAL STATUS
# ============================================================

print("\nGenerated files:")

print(
    "- network_alerts.csv"
)

print(
    "- rule_based_alert_summary.csv"
)

print(
    "- network_alerts.log"
)

print("\n========== PROCESS COMPLETE ==========")

logger.info(
    "Network alert pipeline completed successfully."
)