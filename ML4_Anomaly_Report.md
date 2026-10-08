# ML4 — Anomaly Detection Report

## Objective

Detect unusually high or unusually low network activity by comparing each grid's activity with its historical behaviour for the same hour of day.

The anomaly mechanism is an operational investigation signal. It does not prove network congestion, failure, or a specific root cause.

## Data

Source:

- `data/network_analytics.db`
- `hourly_grid_summary`

Total records analysed:

- 1,679,994

Distinct grids:

- 10,000

Historical period:

- 2013-11-01 00:00:00
- 2013-11-07 23:00:00

## Baseline

The baseline is calculated separately for each:

- `grid_id`
- hour-of-day

The historical median is used as the reference activity level.

A robust historical MAD-based score is also calculated to reduce sensitivity to extreme observations.

## Anomaly Direction

Two directions are retained:

- `HIGH_ANOMALY` — activity is unusually above the historical baseline.
- `LOW_ANOMALY` — activity is unusually below the historical baseline.

Normal observations are labelled:

- `NORMAL`

## Results

Total observations:

- 1,679,994

Normal:

- 1,541,793

Anomalous:

- 138,201

High anomalies:

- 133,945

Low anomalies:

- 4,256

Overall anomaly rate:

- approximately 8.23%

## Three-Way Comparison

The anomaly table also compares ML4 with the ML3 classifier and NP3 rule-based alerts.

Observed combinations:

| Comparison | Records |
|---|---:|
| NONE | 1,505,795 |
| ML4_ONLY | 137,652 |
| ML3_ONLY | 35,998 |
| ML3_ML4 | 549 |

The mechanisms are expected to disagree because they answer different operational questions.

ML3 is a supervised forward-looking risk classifier.

ML4 is a historical behaviour anomaly detector.

NP3 is a rule-based alert mechanism.

Therefore disagreement is informative rather than automatically indicating an error.

## Example High Anomalies

Examples include:

- Grid 4574 at 2013-11-04 07:00 — approximately 2,486.8% above historical median.
- Grid 4574 at 2013-11-04 06:00 — approximately 2,418.0% above historical median.
- Grid 7052 at 2013-11-05 11:00 — approximately 2,242.7% above historical median.
- Grid 7724 at 2013-11-06 14:00 — approximately 2,213.0% above historical median.

These examples demonstrate why both percentage deviation and a robust anomaly score are retained.

## Operational Interpretation

An anomaly means:

> Investigate this grid because its observed activity differs substantially from its historical behaviour for the same hour.

It does not mean:

> The network is congested.

It also does not identify the root cause of the deviation.

Possible explanations include unusual customer behaviour, events, equipment issues, maintenance, traffic shifts, or other operational conditions.

## Why ML3 and ML4 Are Both Useful

ML3 answers:

> Which grids are at elevated risk in the next hourly interval?

ML4 answers:

> Which grids are behaving unusually compared with their historical pattern?

Together they provide complementary predictive and diagnostic signals.

## Limitations

The available dataset contains activity measurements but no network-capacity, outage, maintenance, event, or ground-truth incident labels.

Therefore:

- congestion cannot be directly inferred;
- anomalies are not confirmed incidents;
- ML4 is a decision-support mechanism;
- operational investigation is required before taking action.

## Expected Operational Action

The appropriate action for an ML4 anomaly is:

**Investigate the grid.**

Not:

**Declare the network congested.**

## Output

Generated artefacts:

- `network_anomaly_scores.csv`
- `data/network_analytics.db`
- database table: `network_anomaly_scores`

ML4 status:

**COMPLETE**
