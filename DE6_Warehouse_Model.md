# DE6 – Warehouse Modelling for Network Analytics

## 1. Measures and Dimensions

The `hourly_grid_summary` Spark output contains activity measures such as:

- sms_in
- sms_out
- call_in
- call_out
- internet_activity
- total_activity
- record_count
- avg_activity_per_record

The main dimensions are:

- Time
- Grid

## 2. Fact Table

`fact_network_activity` stores activity measures at the grid and time level.

Keys:

- time_key
- grid_key

Measures:

- sms_in
- sms_out
- call_in
- call_out
- internet_activity
- total_activity
- record_count
- avg_activity_per_record

## 3. Time Dimension

`dim_time` contains:

- time_key
- timestamp
- date
- hour
- day_of_week

## 4. Grid Dimension

`dim_grid` contains:

- grid_key
- grid_id
- centroid_latitude
- centroid_longitude
- geometry_reference

Full Polygon geometry is not repeated in the fact table.

## 5. Storage

SQLite is used as the warehouse because it is appropriate for the current local development environment.

Database:

`data/network_analytics.db`

## 6. Loading Strategy

The Spark hourly summary is used as the source for the fact table.

The grid dimension is populated once from the static Milan reference and reused by the fact table.

## 7. Indexing

Indexes are created on:

- fact_network_activity.time_key
- fact_network_activity.grid_key
- dim_time.date
- dim_grid.grid_id

These support common filtering and joins.

## 8. Example Analytics

The warehouse supports queries for:

- Top grids by total activity
- Activity trends by hour
- Internet-heavy time windows

## 9. Model

```text
              dim_time
                  |
                  |
             fact_network_activity
                  |
                  |
              dim_grid
              