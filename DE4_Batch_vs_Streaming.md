# DE4 – Batch vs Streaming Architecture

## 1. Batch vs Streaming Classification

| Use Case | Processing Type | Reason |
|---|---|---|
| Daily network usage summary | Batch | Data arrives as daily CSV files |
| Live network activity events | Streaming | Events need near real-time processing |
| Billing report | Batch | Billing is generated periodically |
| Hotspot alerts | Streaming | Alerts need to be generated immediately |
| Executive dashboard refresh | Batch | Dashboard can refresh periodically |
| Model training | Batch | Training uses historical data |
| Model scoring | Batch / Streaming | Depends on whether predictions are real-time |

## 2. Current NP1 Pipeline

The current NP1 pipeline is a batch pipeline because the network activity data arrives as CSV files.

```text
CSV Files
    ↓
Airflow
    ↓
data/raw/
    ↓
PySpark
    ↓
data/processed/
    ↓
data/analytics/