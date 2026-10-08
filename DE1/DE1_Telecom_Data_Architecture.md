# DE1 — Telecom Data Architecture

## 1. Purpose

Design a repeatable telecom network intelligence pipeline from daily activity data to analytics and consumer applications before automation.

## 2. Source Data

- Daily activity source: `sms-call-internet-mi-*.csv`
- Static reference data: `milano-grid.geojson`
- The GeoJSON is reference data and is NOT treated as a daily ingestion file.

## 3. Architecture

Daily Activity CSV
        |
        v
    LANDING
        |
        v
 Quality Gate 1
        |
   +----+----+
   |         |
 PASS       FAIL
   |         |
   v         v
  RAW     REJECTED
   |
   +----------------------+
   |                      |
   v                      v
 SPARK          STATIC REFERENCE
                milano-grid.geojson
   |                      |
   +----------+-----------+
              |

         PROCESSED
              |
              v
       Quality Gate 2
              |
              v
          ANALYTICS
              |
       +------+------+------+
       |      |      |      |
       v      v      v      v
    Hourly  Daily  Hotspots Alerts
    Summary Summary          |
                             v
                            Risk
              |
              v
           FastAPI
              |
              v
            React

ML → Produces predictions, anomaly detection or risk scores.
Claude → Explains analytics and ML results in natural language.
Airflow → Orchestrates the pipeline workflow.
SQL → Stores and queries analytics data.

## 4. Data Layers

| Layer | Responsibility | Format / Tool |
|---|---|---|
| Landing | Receives daily activity files | CSV |
| Raw | Stores accepted source data | CSV/Parquet |
| Reference | Stores static geographical reference data | GeoJSON |
| Processed | Stores cleaned, transformed and enriched data | Parquet / Spark |
| Analytics | Stores business-ready analytics outputs | SQL tables |

## 5. Quality Gates

### Quality Gate 1 — Before Raw Acceptance

Check:
- File is readable
- Correct filename
- Required columns exist
- Data types are valid
- Timestamps are valid
- No corrupted data

PASS → Raw  
FAIL → Rejected

### Quality Gate 2 — Before Analytics Publication

Check:
- Required fields exist
- Grid IDs are valid
- Timestamps are valid
- Nulls are within acceptable limits
- Geographic enrichment is successful
- Aggregations are valid

PASS → Analytics  
FAIL → Do not publish

## 6. Analytics Outputs

- `hourly_grid_summary` — hourly activity summarized by grid.
- `daily_grid_summary` — daily activity summarized by grid.
- `hotspots` — grids showing unusually high activity.
- `alerts` — network activity requiring attention.
- `risk` — risk information generated from network activity and ML results.

## 7. Component Responsibilities

| Component | Responsibility |
|--
-|---|
| Spark | Cleans, transforms, aggregates and enriches telecom data. |
| SQL | Stores and queries analytics data. |
| Airflow | Schedules and orchestrates pipeline tasks. |
| FastAPI | Provides APIs for accessing analytics and ML results. |
| React | Displays network intelligence through the dashboard. |
| ML | Produces predictions, anomaly detection or risk scores. |
| Claude | Converts analytics and ML results into natural-language explanations. |

## 8. Assumptions

1. Daily telecom activity arrives as CSV files.
2. `milano-grid.geojson` is static reference data.
3. Activity records contain a grid identifier.
4. Activity data can be linked to the geographical reference using the grid identifier.
5. Analytics data must be available to downstream consumers.

## 9. Non-Goals

- No Spark implementation.
- No SQL implementation.
- No Airflow DAG implementation.
- No FastAPI implementation.
- No React implementation.
- No ML training.
- No Claude integration.
- No production deployment.