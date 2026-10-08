# ML3 — Risk Classifier Evaluation

## Model

Logistic Regression with standardized features and `class_weight=balanced`.

The model predicts whether the **next hourly interval (t+1)** will have unusually high activity.

This is an operational risk proxy, not a claim of physical congestion or capacity failure.

## Target definition

The high-activity threshold was the training-period 90% percentile of t+1 total activity: **1177.6937**.

The threshold was calculated using training data only and then applied unchanged to the test period.

## Chronological split

- Training target range: **2013-11-03 00:00:00 → 2013-11-06 23:00:00**
- Test target range: **2013-11-07 00:00:00 → 2013-11-07 23:00:00**
- Training rows: **959,908**
- Test rows: **239,976**

No random train/test split was used.

## Evaluation

- Accuracy: **0.9450**
- Precision: **0.6730**
- Recall: **0.9515**
- Training base rate: **10.0000%**
- Test base rate: **10.7727%**

### Confusion matrix

| | Actual LOW | Actual HIGH |
|---|---:|---:|
| Predicted LOW | 202,174 | 1,255 |
| Predicted HIGH | 11,950 | 24,597 |

## Feature interpretation

Coefficients are from standardized features. A positive coefficient increases the model's estimated probability of high next-hour risk; a negative coefficient decreases it.

| Feature | Standardized coefficient | Direction |
|---|---:|---|
| avg_activity | 6.173660 | Higher risk |
| variability | -1.424855 | Lower risk |
| peak_ratio | 0.674033 | Higher risk |
| internet_share | -0.123465 | Lower risk |
| activity_growth | 0.016329 | Higher risk |
| active_hours | 0.000000 | Lower risk |

## ML3 vs NP3 rule alerts

- BOTH_FLAG: **0**
- ML_ONLY: **36,547**
- NP3_ONLY: **0**
- NEITHER: **203,429**

NP3 uses a within-day rule baseline. ML3 learns a statistical relationship between engineered features and the next-hour high-activity proxy.

## Three operational observations

1. **ML adds value when it flags risk without an NP3 alert.** These ML_ONLY cases indicate situations where the combined feature pattern suggests elevated next-hour risk even though the within-day rule did not trigger.

2. **Agreement between ML3 and NP3 is stronger evidence for investigation than either mechanism alone.** The two approaches use different logic, so agreement can increase operational confidence without proving a root cause.

3. **ML3 does not replace NP3.** A disagreement may occur because the model learns broader feature relationships while NP3 reacts to explicit activity ratios. Neither mechanism identifies the physical cause of an event, and the current dataset contains only seven days rather than the recommended 14+ days.

## Limitations

- The accumulated dataset currently contains approximately seven days of history; 14 days or more would provide a stronger training basis.
- The target is a synthetic high-activity proxy rather than a measured congestion/capacity outcome.
- The model supports investigation and prioritization; it does not diagnose incidents.
- Precision and recall should be monitored again as more historical data accumulates.

## Artifacts

- Model: `models/ml3_risk_classifier.joblib`
- Predictions: `ml3_predictions.csv`
- Database table: `ml3_predictions`
- Report: `ML3_Model_Evaluation_Report.md`