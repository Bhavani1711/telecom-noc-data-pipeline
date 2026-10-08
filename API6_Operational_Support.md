# API6 Operational Support

## Pipeline Health

Use: GET /pipeline/status

This endpoint provides pipeline health, latest Airflow run, task status, row counts, latest data timestamp (as_of), freshness, and health failure reasons.

### Evidence sources

- data/analytics/pipeline_status.json
- data/network_analytics.db

## Grid Location

Use: GET /network/grid/{grid_id}/location

This endpoint provides grid ID, centroid latitude, centroid longitude, and geometry reference.

### Evidence source

- dim_grid in data/network_analytics.db
- data/reference/milano-grid.geojson

## Operational Note

Raw daily CSV files, Spark jobs, Airflow DAG files, and ML feature files are not required by the API6 endpoints at request time.
