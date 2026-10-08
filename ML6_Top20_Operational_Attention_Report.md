# ML6 Top-20 Operational Attention Report

## Objective

Rank the latest grid-level predictions by trained ML3 risk probability and provide ML4 anomaly evidence for operational investigation.

## Scoring snapshot

- Feature/scoring timestamp: `2013-11-07 23:00:00`
- Model version: `ml3-logistic-v1`
- Top-20 high-risk records: `20`
- Top-20 records with ML4 anomaly flags: `0`

## Top 20

| Rank | Grid | Risk score | Risk level | ML4 anomaly | Direction | Anomaly score |
|---:|---:|---:|---|---:|---|---:|
| 1 | 5458 | 1.000000 | HIGH | 0 | NORMAL | 2.985219 |
| 2 | 4459 | 1.000000 | HIGH | 0 | NORMAL | 2.638822 |
| 3 | 4955 | 1.000000 | HIGH | 0 | NORMAL | 2.318915 |
| 4 | 5061 | 1.000000 | HIGH | 0 | NORMAL | 2.029605 |
| 5 | 4956 | 1.000000 | HIGH | 0 | NORMAL | 1.826055 |
| 6 | 4856 | 1.000000 | HIGH | 0 | NORMAL | 1.773892 |
| 7 | 5059 | 1.000000 | HIGH | 0 | NORMAL | 1.674519 |
| 8 | 5259 | 1.000000 | HIGH | 0 | NORMAL | 1.387408 |
| 9 | 5758 | 1.000000 | HIGH | 0 | NORMAL | 1.326895 |
| 10 | 4961 | 1.000000 | HIGH | 0 | NORMAL | 1.242659 |
| 11 | 6064 | 1.000000 | HIGH | 0 | NORMAL | 0.883958 |
| 12 | 4855 | 1.000000 | HIGH | 0 | NORMAL | 0.878139 |
| 13 | 5256 | 1.000000 | HIGH | 0 | NORMAL | 0.856399 |
| 14 | 4857 | 1.000000 | HIGH | 0 | NORMAL | 0.787489 |
| 15 | 6058 | 1.000000 | HIGH | 0 | NORMAL | 0.768978 |
| 16 | 5567 | 1.000000 | HIGH | 0 | NORMAL | 0.720023 |
| 17 | 5258 | 1.000000 | HIGH | 0 | NORMAL | 0.695912 |
| 18 | 5159 | 1.000000 | HIGH | 0 | NORMAL | 0.548672 |
| 19 | 5857 | 1.000000 | HIGH | 0 | NORMAL | 0.459933 |
| 20 | 5261 | 1.000000 | HIGH | 0 | NORMAL | 0.230803 |

## Operational interpretation

Higher risk scores indicate higher model-estimated probability of elevated next-hour activity risk. ML4 anomaly information provides an independent historical-deviation signal.

These outputs are investigation signals and do not prove congestion, service failure, or a network fault.

## Outputs

- `ml6_top20_operational_attention.csv`
- `ML6_Top20_Operational_Attention_Report.md`
- `network_risk_scores` SQLite table
