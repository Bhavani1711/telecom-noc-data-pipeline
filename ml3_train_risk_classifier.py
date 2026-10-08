import sqlite3
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, confusion_matrix
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


# ============================================================
# CONFIGURATION
# ============================================================

DB_PATH = Path("data/network_analytics.db")

MODEL_DIR = Path("models")
MODEL_PATH = MODEL_DIR / "ml3_risk_classifier.joblib"

PREDICTIONS_FILE = Path("ml3_predictions.csv")
REPORT_FILE = Path("ML3_Model_Evaluation_Report.md")

FEATURE_TABLE = "network_feature_table"
ALERT_FILE = Path("network_alerts.csv")

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]

TRAIN_FRACTION = 0.80
RISK_PERCENTILE = 0.90


# ============================================================
# 1. LOAD ENGINEERED FEATURES
# ============================================================

if not DB_PATH.exists():
    raise FileNotFoundError(
        f"Database not found: {DB_PATH}"
    )

with sqlite3.connect(DB_PATH) as conn:

    features = pd.read_sql_query(
        f"""
        SELECT
            grid_id,
            feature_timestamp,
            avg_activity,
            activity_growth,
            active_hours,
            peak_ratio,
            variability,
            internet_share
        FROM {FEATURE_TABLE}
        ORDER BY feature_timestamp, grid_id
        """,
        conn,
    )

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


if features.empty:
    raise ValueError(
        "network_feature_table contains no rows."
    )

if activity.empty:
    raise ValueError(
        "hourly_grid_summary contains no rows."
    )


# ============================================================
# 2. PREPARE TIMESTAMPS
# ============================================================

features["feature_timestamp"] = pd.to_datetime(
    features["feature_timestamp"],
    errors="coerce",
)

activity["timestamp"] = pd.to_datetime(
    activity["timestamp"],
    errors="coerce",
)

features = features.dropna(
    subset=["feature_timestamp"]
).copy()

activity = activity.dropna(
    subset=["timestamp"]
).copy()


# ============================================================
# 3. BUILD t+1 TARGET
# ============================================================

# The feature row at time t predicts the next hourly interval t+1.
#
# We deliberately construct the target from the activity table
# rather than using any future activity as a model feature.

target_lookup = activity.rename(
    columns={
        "timestamp": "target_timestamp",
        "total_activity": "target_activity",
    }
)

features["target_timestamp"] = (
    features["feature_timestamp"]
    + pd.Timedelta(hours=1)
)

model_data = features.merge(
    target_lookup,
    on=["grid_id", "target_timestamp"],
    how="inner",
)

model_data = model_data.sort_values(
    ["target_timestamp", "grid_id"]
).reset_index(drop=True)


if model_data.empty:
    raise ValueError(
        "No feature rows could be matched to a t+1 target."
    )


# ============================================================
# 4. CHRONOLOGICAL TRAIN / TEST SPLIT
# ============================================================

timestamps = np.sort(
    model_data["target_timestamp"].unique()
)

if len(timestamps) < 3:
    raise ValueError(
        "Not enough distinct timestamps for a chronological "
        "train/test split."
    )

split_position = int(
    len(timestamps) * TRAIN_FRACTION
)

split_position = max(
    1,
    min(split_position, len(timestamps) - 1)
)

split_timestamp = pd.Timestamp(
    timestamps[split_position]
)

train = model_data[
    model_data["target_timestamp"] < split_timestamp
].copy()

test = model_data[
    model_data["target_timestamp"] >= split_timestamp
].copy()


# ============================================================
# 5. CREATE TARGET USING TRAINING DATA ONLY
# ============================================================

risk_threshold = train[
    "target_activity"
].quantile(RISK_PERCENTILE)

train["high_activity_risk"] = (
    train["target_activity"] >= risk_threshold
).astype(int)

test["high_activity_risk"] = (
    test["target_activity"] >= risk_threshold
).astype(int)


# ============================================================
# 6. VALIDATE CLASS BALANCE
# ============================================================

if train["high_activity_risk"].nunique() < 2:
    raise ValueError(
        "Training target contains only one class. "
        "A classifier cannot be trained."
    )

if test["high_activity_risk"].nunique() < 2:
    print(
        "WARNING: Test target contains only one class. "
        "Precision/recall may be limited."
    )


# ============================================================
# 7. PREPARE FEATURES
# ============================================================

X_train = train[FEATURE_COLUMNS].copy()
X_test = test[FEATURE_COLUMNS].copy()

y_train = train["high_activity_risk"]
y_test = test["high_activity_risk"]

# Numeric safety.
X_train = X_train.replace(
    [np.inf, -np.inf],
    np.nan,
).fillna(0)

X_test = X_test.replace(
    [np.inf, -np.inf],
    np.nan,
).fillna(0)


# ============================================================
# 8. TRAIN INTERPRETABLE LOGISTIC REGRESSION
# ============================================================

model = Pipeline(
    steps=[
        (
            "scaler",
            StandardScaler(),
        ),
        (
            "classifier",
            LogisticRegression(
                max_iter=1000,
                class_weight="balanced",
                random_state=42,
            ),
        ),
    ]
)

model.fit(
    X_train,
    y_train,
)


# ============================================================
# 9. GENERATE TEST PREDICTIONS
# ============================================================

test["risk_probability"] = model.predict_proba(
    X_test
)[:, 1]

test["model_prediction"] = model.predict(
    X_test
).astype(int)

test["model_risk_level"] = np.where(
    test["model_prediction"] == 1,
    "HIGH",
    "LOW",
)


# ============================================================
# 10. EVALUATE MODEL
# ============================================================

accuracy = accuracy_score(
    y_test,
    test["model_prediction"],
)

precision = precision_score(
    y_test,
    test["model_prediction"],
    zero_division=0,
)

recall = recall_score(
    y_test,
    test["model_prediction"],
    zero_division=0,
)

train_base_rate = y_train.mean()
test_base_rate = y_test.mean()

tn, fp, fn, tp = confusion_matrix(
    y_test,
    test["model_prediction"],
    labels=[0, 1],
).ravel()


# ============================================================
# 11. INSPECT STANDARDIZED COEFFICIENTS
# ============================================================

classifier = model.named_steps["classifier"]

coefficients = pd.DataFrame(
    {
        "feature": FEATURE_COLUMNS,
        "coefficient": classifier.coef_[0],
    }
)

coefficients["abs_coefficient"] = (
    coefficients["coefficient"].abs()
)

coefficients = coefficients.sort_values(
    "abs_coefficient",
    ascending=False,
).reset_index(drop=True)


# ============================================================
# 12. COMPARE AGAINST NP3 ALERTS
# ============================================================

if ALERT_FILE.exists():

    np3_alerts = pd.read_csv(
        ALERT_FILE
    )

else:

    np3_alerts = pd.DataFrame(
        columns=[
            "grid_id",
            "timestamp",
            "alert_type",
        ]
    )


if not np3_alerts.empty:

    np3_alerts["timestamp"] = pd.to_datetime(
        np3_alerts["timestamp"],
        errors="coerce",
    )

    np3_keys = (
        np3_alerts[
            ["grid_id", "timestamp"]
        ]
        .dropna()
        .drop_duplicates()
        .assign(np3_alert=1)
    )

else:

    np3_keys = pd.DataFrame(
        columns=[
            "grid_id",
            "timestamp",
            "np3_alert",
        ]
    )


test["np3_alert"] = 0

if not np3_keys.empty:

    test = test.merge(
        np3_keys.rename(
            columns={
                "timestamp": "target_timestamp",
            }
        ),
        on=["grid_id", "target_timestamp"],
        how="left",
        suffixes=("", "_from_np3"),
    )

    if "np3_alert_from_np3" in test.columns:

        test["np3_alert"] = (
            test["np3_alert_from_np3"]
            .fillna(0)
            .astype(int)
        )

        test = test.drop(
            columns=["np3_alert_from_np3"]
        )


test["comparison"] = np.select(
    [
        (test["model_prediction"] == 1)
        & (test["np3_alert"] == 1),

        (test["model_prediction"] == 1)
        & (test["np3_alert"] == 0),

        (test["model_prediction"] == 0)
        & (test["np3_alert"] == 1),

        (test["model_prediction"] == 0)
        & (test["np3_alert"] == 0),
    ],
    [
        "BOTH_FLAG",
        "ML_ONLY",
        "NP3_ONLY",
        "NEITHER",
    ],
    default="UNKNOWN",
)


comparison_counts = (
    test["comparison"]
    .value_counts()
    .to_dict()
)


# ============================================================
# 13. SAVE PREDICTIONS
# ============================================================

prediction_columns = [
    "grid_id",
    "feature_timestamp",
    "target_timestamp",
    "target_activity",
    "high_activity_risk",
    "risk_probability",
    "model_prediction",
    "model_risk_level",
    "np3_alert",
    "comparison",
]

test[prediction_columns].to_csv(
    PREDICTIONS_FILE,
    index=False,
)


# ============================================================
# 14. SAVE MODEL
# ============================================================

MODEL_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

joblib.dump(
    {
        "model": model,
        "feature_columns": FEATURE_COLUMNS,
        "risk_threshold": float(risk_threshold),
        "risk_percentile": RISK_PERCENTILE,
        "target_definition": (
            "HIGH_ACTIVITY_RISK=1 when t+1 total activity "
            "is greater than or equal to the training-period "
            "90th percentile threshold."
        ),
        "prediction_unit": "grid + next hourly interval",
    },
    MODEL_PATH,
)


# ============================================================
# 15. STORE PREDICTIONS IN DATABASE
# ============================================================

with sqlite3.connect(DB_PATH) as conn:

    test[prediction_columns].to_sql(
        "ml3_predictions",
        conn,
        if_exists="replace",
        index=False,
    )


# ============================================================
# 16. GENERATE EVALUATION REPORT
# ============================================================

train_start = train["target_timestamp"].min()
train_end = train["target_timestamp"].max()

test_start = test["target_timestamp"].min()
test_end = test["target_timestamp"].max()

report_lines = [
    "# ML3 — Risk Classifier Evaluation",
    "",
    "## Model",
    "",
    "Logistic Regression with standardized features and "
    "`class_weight=balanced`.",
    "",
    "The model predicts whether the **next hourly interval "
    "(t+1)** will have unusually high activity.",
    "",
    "This is an operational risk proxy, not a claim of "
    "physical congestion or capacity failure.",
    "",
    "## Target definition",
    "",
    f"The high-activity threshold was the training-period "
    f"{RISK_PERCENTILE:.0%} percentile of t+1 total activity: "
    f"**{risk_threshold:.4f}**.",
    "",
    "The threshold was calculated using training data only "
    "and then applied unchanged to the test period.",
    "",
    "## Chronological split",
    "",
    f"- Training target range: **{train_start} → {train_end}**",
    f"- Test target range: **{test_start} → {test_end}**",
    f"- Training rows: **{len(train):,}**",
    f"- Test rows: **{len(test):,}**",
    "",
    "No random train/test split was used.",
    "",
    "## Evaluation",
    "",
    f"- Accuracy: **{accuracy:.4f}**",
    f"- Precision: **{precision:.4f}**",
    f"- Recall: **{recall:.4f}**",
    f"- Training base rate: **{train_base_rate:.4%}**",
    f"- Test base rate: **{test_base_rate:.4%}**",
    "",
    "### Confusion matrix",
    "",
    "| | Actual LOW | Actual HIGH |",
    "|---|---:|---:|",
    f"| Predicted LOW | {tn:,} | {fn:,} |",
    f"| Predicted HIGH | {fp:,} | {tp:,} |",
    "",
    "## Feature interpretation",
    "",
    "Coefficients are from standardized features. "
    "A positive coefficient increases the model's estimated "
    "probability of high next-hour risk; a negative coefficient "
    "decreases it.",
    "",
    "| Feature | Standardized coefficient | Direction |",
    "|---|---:|---|",
]

for _, row in coefficients.iterrows():

    direction = (
        "Higher risk"
        if row["coefficient"] > 0
        else "Lower risk"
    )

    report_lines.append(
        f"| {row['feature']} | "
        f"{row['coefficient']:.6f} | "
        f"{direction} |"
    )


report_lines.extend(
    [
        "",
        "## ML3 vs NP3 rule alerts",
        "",
        f"- BOTH_FLAG: **{comparison_counts.get('BOTH_FLAG', 0):,}**",
        f"- ML_ONLY: **{comparison_counts.get('ML_ONLY', 0):,}**",
        f"- NP3_ONLY: **{comparison_counts.get('NP3_ONLY', 0):,}**",
        f"- NEITHER: **{comparison_counts.get('NEITHER', 0):,}**",
        "",
        "NP3 uses a within-day rule baseline. ML3 learns a "
        "statistical relationship between engineered features "
        "and the next-hour high-activity proxy.",
        "",
        "## Three operational observations",
        "",
        "1. **ML adds value when it flags risk without an NP3 alert.** "
        "These ML_ONLY cases indicate situations where the combined "
        "feature pattern suggests elevated next-hour risk even though "
        "the within-day rule did not trigger.",
        "",
        "2. **Agreement between ML3 and NP3 is stronger evidence "
        "for investigation than either mechanism alone.** "
        "The two approaches use different logic, so agreement can "
        "increase operational confidence without proving a root cause.",
        "",
        "3. **ML3 does not replace NP3.** "
        "A disagreement may occur because the model learns broader "
        "feature relationships while NP3 reacts to explicit activity "
        "ratios. Neither mechanism identifies the physical cause of "
        "an event, and the current dataset contains only seven days "
        "rather than the recommended 14+ days.",
        "",
        "## Limitations",
        "",
        "- The accumulated dataset currently contains approximately "
        "seven days of history; 14 days or more would provide a stronger "
        "training basis.",
        "- The target is a synthetic high-activity proxy rather than "
        "a measured congestion/capacity outcome.",
        "- The model supports investigation and prioritization; it "
        "does not diagnose incidents.",
        "- Precision and recall should be monitored again as more "
        "historical data accumulates.",
        "",
        "## Artifacts",
        "",
        f"- Model: `{MODEL_PATH}`",
        f"- Predictions: `{PREDICTIONS_FILE}`",
        f"- Database table: `ml3_predictions`",
        f"- Report: `{REPORT_FILE}`",
    ]
)

REPORT_FILE.write_text(
    "\n".join(report_lines)
)


# ============================================================
# 17. CONSOLE SUMMARY
# ============================================================

print()
print("=" * 60)
print("ML3 RISK CLASSIFIER COMPLETE")
print("=" * 60)

print(f"Feature rows used:       {len(model_data):,}")
print(f"Training rows:           {len(train):,}")
print(f"Test rows:               {len(test):,}")

print()
print("Chronological split:")
print(f"Train: {train_start} -> {train_end}")
print(f"Test:  {test_start} -> {test_end}")

print()
print(f"Risk threshold:          {risk_threshold:.4f}")
print(f"Training base rate:      {train_base_rate:.2%}")
print(f"Test base rate:          {test_base_rate:.2%}")

print()
print("Evaluation:")
print(f"Accuracy:                {accuracy:.4f}")
print(f"Precision:               {precision:.4f}")
print(f"Recall:                  {recall:.4f}")

print()
print("ML3 vs NP3:")
print(f"BOTH_FLAG:               {comparison_counts.get('BOTH_FLAG', 0):,}")
print(f"ML_ONLY:                 {comparison_counts.get('ML_ONLY', 0):,}")
print(f"NP3_ONLY:                {comparison_counts.get('NP3_ONLY', 0):,}")
print(f"NEITHER:                 {comparison_counts.get('NEITHER', 0):,}")

print()
print("Top model coefficients:")

for _, row in coefficients.head(6).iterrows():

    print(
        f"  {row['feature']:<20} "
        f"{row['coefficient']:+.6f}"
    )

print()
print("Generated:")
print(f"- {MODEL_PATH}")
print(f"- {PREDICTIONS_FILE}")
print(f"- {REPORT_FILE}")
print("- database table: ml3_predictions")

print()
print("PROCESS COMPLETE")
