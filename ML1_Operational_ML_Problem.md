# ML1 — Operational ML Problem

## Primary Training Problem

Predict whether a grid is at elevated risk of unusually high network activity in the next hourly interval.

The model is a decision-support tool for investigation. It does not claim that a grid is congested because the dataset does not contain network capacity information.

## Candidate Problems Considered

### 1. High-activity risk — SELECTED

Predict whether activity in the next hour will exceed a defined historical training threshold.

Reason for selection:
- directly supports the NOC hotspot workflow
- produces an actionable forward-looking signal
- can be trained using the available historical activity data

### 2. Anomalous activity

Identify whether future activity is unusual relative to historical behavior.

This is useful for monitoring, but is not selected as the primary supervised training problem because the anomaly definition would require a more complex reference strategy.

### 3. Activity drop

Predict whether activity will fall significantly in the next hour.

This is useful for investigating possible service degradation, but is not selected as the primary problem.

## Prediction Unit

One grid plus one future hourly interval.

For a prediction at time t+1:
- features use historical activity from the trailing window ending at t
- the target describes activity during t+1

## Temporal Boundary

Features:
[t-window+1 ... t]

Target:
[t+1]

No feature may use information after t.

## Target Strategy

The target is a synthetic training proxy because the dataset does not contain real congestion or capacity labels.

HIGH_ACTIVITY_RISK = 1 when the next-hour total activity exceeds the documented historical activity threshold.

HIGH_ACTIVITY_RISK = 0 otherwise.

The threshold is a training proxy for unusually high activity and must not be interpreted as a physical network-capacity limit.

## Business Action

The model supports the operational action:

"Investigate the grid."

The model must not claim:

"The network is congested."

## Leakage Risks

Potential leakage includes:

1. Using t+1 activity in the features.
2. Calculating a rolling average that includes future intervals.
3. Calculating normalization or thresholds using information from the prediction period.
4. Randomly splitting time-dependent observations so future observations influence training of earlier observations.

The t/t+1 boundary prevents direct feature leakage because all features stop at t while the target begins at t+1.

Time-aware train/test splitting will be used during model development.

## Evidence Available

The prediction uses historical network activity from the warehouse, particularly hourly_grid_summary.

No network capacity information is available, so congestion cannot be directly inferred.
