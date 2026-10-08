import sqlite3
from pathlib import Path
import json

import pandas as pd


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

DB_PATH = BASE_DIR / "data" / "network_analytics.db"
FACT_SOURCE = BASE_DIR / "sp7_output" / "hourly_grid_summary"
REFERENCE_FILE = BASE_DIR / "data" / "reference" / "milano-grid.geojson"


# ============================================================
# 1. READ SPARK OUTPUT
# ============================================================

print("Reading Spark output...")

df = pd.read_parquet(FACT_SOURCE)

print("Spark output rows:", len(df))
print("Columns:", list(df.columns))


# ============================================================
# 2. CREATE TIMESTAMP FROM DATE + HOUR
# ============================================================
# Spark output contains "date" and "hour", not "timestamp".

df["timestamp"] = pd.to_datetime(
    df["date"].astype(str)
    + " "
    + df["hour"].astype(str)
    + ":00:00"
)


# ============================================================
# 3. CONNECT TO SQLITE
# ============================================================

conn = sqlite3.connect(DB_PATH)


# ============================================================
# 4. CREATE WAREHOUSE TABLES
# ============================================================

conn.executescript("""
DROP TABLE IF EXISTS fact_network_activity;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_grid;

CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,
    timestamp TEXT NOT NULL,
    date TEXT NOT NULL,
    hour INTEGER NOT NULL,
    day_of_week INTEGER
);

CREATE TABLE dim_grid (
    grid_key INTEGER PRIMARY KEY AUTOINCREMENT,
    grid_id INTEGER UNIQUE NOT NULL,
    centroid_latitude REAL,
    centroid_longitude REAL,
    geometry_reference TEXT
);

CREATE TABLE fact_network_activity (
    time_key INTEGER NOT NULL,
    grid_key INTEGER NOT NULL,

    sms_in REAL,
    sms_out REAL,
    call_in REAL,
    call_out REAL,
    internet_activity REAL,
    total_activity REAL,
    record_count INTEGER,
    avg_activity_per_record REAL,

    PRIMARY KEY (time_key, grid_key),

    FOREIGN KEY (time_key)
        REFERENCES dim_time(time_key),

    FOREIGN KEY (grid_key)
        REFERENCES dim_grid(grid_key)
);
""")


# ============================================================
# 5. PREPARE TIME DIMENSION
# ============================================================

time_df = (
    df[["timestamp", "hour"]]
    .drop_duplicates()
    .sort_values("timestamp")
    .reset_index(drop=True)
)

time_df["time_key"] = range(1, len(time_df) + 1)

time_df["date"] = (
    time_df["timestamp"]
    .dt.strftime("%Y-%m-%d")
)

time_df["day_of_week"] = (
    time_df["timestamp"]
    .dt.dayofweek + 1
)

time_df = time_df[
    [
        "time_key",
        "timestamp",
        "date",
        "hour",
        "day_of_week",
    ]
]

# Convert timestamp to text for SQLite


time_df.to_sql(
    "dim_time",
    conn,
    if_exists="append",
    index=False,
)


# ============================================================
# 6. PREPARE GRID DIMENSION
# ============================================================

grid_df = (
    df[["grid_id"]]
    .drop_duplicates()
    .sort_values("grid_id")
    .reset_index(drop=True)
)

grid_df["grid_key"] = range(1, len(grid_df) + 1)

grid_df["centroid_latitude"] = None
grid_df["centroid_longitude"] = None
grid_df["geometry_reference"] = None


# ============================================================
# 7. GEOJSON REFERENCE
# ============================================================

if REFERENCE_FILE.exists():

    try:

        with open(
            REFERENCE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            geojson = json.load(f)

        for feature in geojson.get("features", []):

            properties = feature.get(
                "properties",
                {}
            )

            cell_id = properties.get("cellId")

            if cell_id is not None:

                grid_df.loc[
                    grid_df["grid_id"] == int(cell_id),
                    "geometry_reference"
                ] = (
                    "milano-grid.geojson:"
                    f"cellId={cell_id}"
                )

        print("GeoJSON reference loaded.")

    except Exception as e:

        print(
            "GeoJSON enrichment skipped:",
            e
        )

else:

    print(
        "GeoJSON reference file not found. "
        "Continuing without geometry references."
    )


grid_df = grid_df[
    [
        "grid_key",
        "grid_id",
        "centroid_latitude",
        "centroid_longitude",
        "geometry_reference",
    ]
]

grid_df.to_sql(
    "dim_grid",
    conn,
    if_exists="append",
    index=False,
)


# ============================================================
# 8. CREATE FACT TABLE
# ============================================================

fact_df = df.merge(
    time_df[
        ["time_key", "timestamp"]
    ],
    left_on="timestamp",
    right_on="timestamp",
    how="left",
)

fact_df = fact_df.merge(
    grid_df[
        ["grid_key", "grid_id"]
    ],
    on="grid_id",
    how="left",
)


fact_df = fact_df[
    [
        "time_key",
        "grid_key",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
        "total_activity",
        "record_count",
        "avg_activity_per_record",
    ]
]


fact_df.to_sql(
    "fact_network_activity",
    conn,
    if_exists="append",
    index=False,
)


# ============================================================
# 9. CREATE INDEXES
# ============================================================

conn.executescript("""
CREATE INDEX IF NOT EXISTS idx_fact_time
ON fact_network_activity(time_key);

CREATE INDEX IF NOT EXISTS idx_fact_grid
ON fact_network_activity(grid_key);

CREATE INDEX IF NOT EXISTS idx_dim_time_date
ON dim_time(date);

CREATE INDEX IF NOT EXISTS idx_dim_grid_id
ON dim_grid(grid_id);
""")


conn.commit()


# ============================================================
# 10. VALIDATION
# ============================================================

print("\n===== DE6 WAREHOUSE VALIDATION =====")

for table in [
    "dim_time",
    "dim_grid",
    "fact_network_activity",
]:

    count = conn.execute(
        f"SELECT COUNT(*) FROM {table}"
    ).fetchone()[0]

    print(
        f"{table}: {count:,} rows"
    )


# ============================================================
# 11. TOP 10 GRIDS
# ============================================================

print("\nTop 10 grids:")

query = """
SELECT
    grid_key,
    ROUND(SUM(total_activity), 2)
        AS total_activity
FROM fact_network_activity
GROUP BY grid_key
ORDER BY total_activity DESC
LIMIT 10;
"""

top_grids = pd.read_sql_query(
    query,
    conn
)

print(
    top_grids.to_string(
        index=False
    )
)


# ============================================================
# 12. HOURLY TRENDS
# ============================================================

print("\nHourly trends:")

query = """
SELECT
    t.hour,
    ROUND(SUM(f.total_activity), 2)
        AS total_activity
FROM fact_network_activity f
JOIN dim_time t
    ON f.time_key = t.time_key
GROUP BY t.hour
ORDER BY t.hour;
"""

hourly_trends = pd.read_sql_query(
    query,
    conn
)

print(
    hourly_trends.to_string(
        index=False
    )
)


# ============================================================
# 13. INTERNET-HEAVY WINDOWS
# ============================================================

print("\nInternet-heavy windows:")

query = """
SELECT
    t.date,
    t.hour,
    ROUND(SUM(f.internet_activity), 2)
        AS internet_activity
FROM fact_network_activity f
JOIN dim_time t
    ON f.time_key = t.time_key
GROUP BY t.date, t.hour
ORDER BY internet_activity DESC
LIMIT 10;
"""

internet_windows = pd.read_sql_query(
    query,
    conn
)

print(
    internet_windows.to_string(
        index=False
    )
)


# ============================================================
# 14. FINAL CHECK
# ============================================================

print("\nIndexes:")

indexes = conn.execute("""
SELECT
    name
FROM sqlite_master
WHERE type = 'index'
AND name LIKE 'idx_%'
ORDER BY name;
""").fetchall()

for index in indexes:
    print("-", index[0])


# ============================================================
# 15. CLOSE DATABASE
# ============================================================

conn.close()


print("\n===================================")
print("DE6 COMPLETE")
print("===================================")