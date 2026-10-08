# DE5 – Storage Strategy & Data Zones

## 1. Data Zones

| Zone | Content | Format | Purpose |
|---|---|---|---|
| landing/ | Incoming daily CSV files | CSV | Temporary arrival area |
| raw/ | Accepted source CSV files | CSV | Immutable audit copy |
| reference/ | milano-grid.geojson | GeoJSON | Static reference data |
| processed/ | Cleaned and aggregated data | Parquet | Efficient Spark processing |
| analytics/ | Curated analytical outputs | Parquet / warehouse tables | Reporting and querying |
| logs/ | Validation, ingestion and run history | CSV / log | Audit and monitoring |

## 2. Directory Structure

```text
data/
├── landing/
│   └── raw_csv_files/
├── raw/
│   └── sms-call-internet-mi-YYYY-MM-DD.csv
├── reference/
│   └── milano-grid.geojson
├── processed/
│   ├── cleaned_activity/
│   │   └── date=YYYY-MM-DD/
│   └── hourly_grid_summary/
│       └── date=YYYY-MM-DD/
├── analytics/
│   └── dashboard_summary.csv/
└── logs/
    ├── detected_files.txt
    ├── validation_results.csv
    └── ingestion_metadata.csv
    