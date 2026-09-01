
import os
import sys
import json
import shutil
import time
from pathlib import Path

import pandas as pd


# ============================================================
# PYTHON CONFIGURATION
# ============================================================

print("=" * 70)
print("PYTHON CONFIGURATION")
print("=" * 70)

print("Python executable:")
print(sys.executable)

print("=" * 70)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent

SP4_FILE = (
    PROJECT_DIR
    / "sp4_output"
    / "enriched_network_activity"
    / "enriched_network_activity.csv"
)

SP3_DIR = PROJECT_DIR / "sp3_output" / "hourly_grid_summary"

REFERENCE_DIR = PROJECT_DIR / "data" / "reference"

# Possible locations for milano-grid.geojson
GEOJSON_CANDIDATES = [
    REFERENCE_DIR / "milano-grid.geojson",
    PROJECT_DIR / "milano-grid.geojson",
    PROJECT_DIR / "data" / "milano-grid.geojson",
]

OUTPUT_DIR = PROJECT_DIR / "sp6_output"

CLEAN_PARQUET_DIR = OUTPUT_DIR / "clean_activity_parquet"

HOURLY_PARQUET_DIR = OUTPUT_DIR / "hourly_grid_summary_parquet"

STATIC_GRID_DIR = OUTPUT_DIR / "static_grid_reference"

DASHBOARD_DIR = OUTPUT_DIR / "dashboard_summary"

REPORT_FILE = OUTPUT_DIR / "sp6_validation_report.json"


# ============================================================
# CREATE OUTPUT DIRECTORIES
# ============================================================

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
HOURLY_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
STATIC_GRID_DIR.mkdir(parents=True, exist_ok=True)
DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

REFERENCE_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def find_sp3_file():
    """
    Find the SP3 hourly_grid_summary CSV.
    """
    if not SP3_DIR.exists():
        return None

    csv_files = list(SP3_DIR.glob("*.csv"))

    if not csv_files:
        return None

    # Prefer a normal CSV file over Spark part files if both exist
    normal_files = [
        f for f in csv_files
        if not f.name.startswith("part-")
    ]

    if normal_files:
        return normal_files[0]

    return csv_files[0]


def find_geojson():
    """
    Find milano-grid.geojson.
    """
    for candidate in GEOJSON_CANDIDATES:
        if candidate.exists():
            return candidate

    return None


def folder_size_bytes(path):
    """
    Calculate total size of all files in a folder.
    """
    if not path.exists():
        return 0

    total = 0

    if path.is_file():
        return path.stat().st_size

    for file in path.rglob("*"):
        if file.is_file():
            total += file.stat().st_size

    return total


def format_size(size_bytes):
    """
    Convert bytes into readable units.
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"

    if size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.2f} KB"

    if size_bytes < 1024 ** 3:
        return f"{size_bytes / (1024 ** 2):.2f} MB"

    return f"{size_bytes / (1024 ** 3):.2f} GB"


def clean_output(path):
    """
    Remove an existing output safely.
    """
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


# ============================================================
# START
# ============================================================

print()
print("=" * 70)
print("SP6 - PARQUET STORAGE AND VALIDATION")
print("=" * 70)

print()
print("Project directory:")
print(PROJECT_DIR)


# ============================================================
# CHECK INPUT FILES
# ============================================================

print()
print("=" * 70)
print("CHECKING REQUIRED INPUT FILES")
print("=" * 70)

print()
print("SP4 enriched activity:")
print(SP4_FILE)

if not SP4_FILE.exists():
    print()
    print("ERROR: SP4 enriched CSV was not found.")
    print("Expected:")
    print(SP4_FILE)
    sys.exit(1)

SP3_FILE = find_sp3_file()

print()
print("SP3 hourly summary:")

if SP3_FILE is None:
    print("ERROR: SP3 hourly_grid_summary CSV was not found.")
    print("Expected directory:")
    print(SP3_DIR)
    sys.exit(1)

print(SP3_FILE)


GEOJSON_FILE = find_geojson()

print()
print("Milano grid GeoJSON:")

if GEOJSON_FILE is None:
    print("WARNING: milano-grid.geojson was not found.")
    print("The rest of SP6 will continue.")
else:
    print(GEOJSON_FILE)


# ============================================================
# IMPORT PYARROW
# ============================================================

print()
print("=" * 70)
print("CHECKING PARQUET SUPPORT")
print("=" * 70)

try:
    import pyarrow
    import pyarrow.parquet as pq

    print("PyArrow version:", pyarrow.__version__)

except ImportError:
    print()
    print("ERROR: PyArrow is not installed.")
    print()
    print("Run:")
    print("python -m pip install pyarrow")
    print()
    sys.exit(1)


# ============================================================
# STEP 1
# WRITE CLEAN ACTIVITY DATA AS PARQUET
# ============================================================

print()
print("=" * 70)
print("STEP 1 - WRITE CLEAN ACTIVITY DATA AS PARQUET")
print("=" * 70)

print()
print("Loading SP4 enriched activity data...")

activity_df = pd.read_csv(
    SP4_FILE,
    parse_dates=["timestamp"]
)

print()
print("Input rows:")
print(len(activity_df))

print()
print("Input columns:")
print(list(activity_df.columns))


# ------------------------------------------------------------
# Clean data
# ------------------------------------------------------------

required_activity_columns = [
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_activity",
    "geometry",
]


missing_columns = [
    c for c in required_activity_columns
    if c not in activity_df.columns
]

if missing_columns:
    print()
    print("ERROR: Missing required SP4 columns:")
    for col in missing_columns:
        print(" -", col)
    sys.exit(1)


# Create date column
activity_df["date"] = activity_df["timestamp"].dt.date


# Convert numeric columns
numeric_columns = [
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_activity",
]

for column in numeric_columns:
    activity_df[column] = pd.to_numeric(
        activity_df[column],
        errors="coerce"
    )


# Drop rows with invalid critical fields
before_cleaning = len(activity_df)

activity_df = activity_df.dropna(
    subset=[
        "timestamp",
        "grid_id",
        "total_activity",
    ]
).copy()

after_cleaning = len(activity_df)

print()
print("Rows removed during cleaning:")
print(before_cleaning - after_cleaning)

print()
print("Clean activity rows:")
print(len(activity_df))


# ------------------------------------------------------------
# Write Parquet
# ------------------------------------------------------------

clean_output(CLEAN_PARQUET_DIR)
CLEAN_PARQUET_DIR.mkdir(parents=True, exist_ok=True)

clean_parquet_file = CLEAN_PARQUET_DIR / "clean_activity.parquet"

activity_df.to_parquet(
    clean_parquet_file,
    engine="pyarrow",
    index=False
)

print()
print("Clean activity Parquet created:")
print(clean_parquet_file)

print()
print("Parquet size:")
print(format_size(folder_size_bytes(clean_parquet_file)))


# ============================================================
# STEP 2
# PARTITION CLEANED OUTPUT BY DATE
# ============================================================

print()
print("=" * 70)
print("STEP 2 - PARTITION CLEANED OUTPUT BY DATE")
print("=" * 70)

partitioned_activity_dir = (
    OUTPUT_DIR / "clean_activity_partitioned"
)

clean_output(partitioned_activity_dir)
partitioned_activity_dir.mkdir(
    parents=True,
    exist_ok=True
)

print()
print("Writing partitioned Parquet...")
print("Partition column: date")


activity_df.to_parquet(
    partitioned_activity_dir,
    engine="pyarrow",
    index=False,
    partition_cols=["date"]
)

print()
print("Partitioned Parquet created:")
print(partitioned_activity_dir)


# ------------------------------------------------------------
# Show partition folders
# ------------------------------------------------------------

partition_folders = sorted(
    [
        p for p in partitioned_activity_dir.iterdir()
        if p.is_dir()
    ]
)

print()
print("Number of date partitions:")
print(len(partition_folders))

print()
print("Sample partitions:")

for folder in partition_folders[:10]:
    print(" -", folder.name)


# ============================================================
# STEP 3
# WRITE HOURLY_GRID_SUMMARY AS PARQUET
# ============================================================

print()
print("=" * 70)
print("STEP 3 - WRITE HOURLY_GRID_SUMMARY AS PARQUET")
print("=" * 70)

print()
print("Loading SP3 hourly summary...")

hourly_df = pd.read_csv(
    SP3_FILE,
    parse_dates=["timestamp"]
)

print()
print("SP3 rows:")
print(len(hourly_df))

print()
print("SP3 columns:")
print(list(hourly_df.columns))


# ------------------------------------------------------------
# Validate SP3 structure
# ------------------------------------------------------------

required_hourly_columns = [
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet",
    "total_sms_activity",
    "total_call_activity",
    "total_activity",
    "daily_activity",
    "internet_share",
]

missing_hourly_columns = [
    c for c in required_hourly_columns
    if c not in hourly_df.columns
]


# ------------------------------------------------------------
# Handle SP3 naming variation
# ------------------------------------------------------------

if "internet" not in hourly_df.columns:

    if "internet_activity" in hourly_df.columns:

        print()
        print(
            "SP3 contains internet_activity; "
            "renaming to internet."
        )

        hourly_df = hourly_df.rename(
            columns={
                "internet_activity": "internet"
            }
        )

    else:
        print()
        print("ERROR: SP3 internet column is missing.")
        print("Available columns:")
        print(list(hourly_df.columns))
        sys.exit(1)


# Re-check after rename
missing_hourly_columns = [
    c for c in required_hourly_columns
    if c not in hourly_df.columns
]

if missing_hourly_columns:
    print()
    print("ERROR: Missing SP3 columns:")

    for col in missing_hourly_columns:
        print(" -", col)

    sys.exit(1)


# ------------------------------------------------------------
# Ensure one record per grid and hour
# ------------------------------------------------------------

duplicate_count = hourly_df.duplicated(
    subset=["grid_id", "timestamp"]
).sum()

print()
print("Duplicate grid-hour records:")
print(duplicate_count)

if duplicate_count > 0:

    print()
    print("Removing duplicate grid-hour records...")

    hourly_df = (
        hourly_df
        .sort_values(["grid_id", "timestamp"])
        .drop_duplicates(
            subset=["grid_id", "timestamp"],
            keep="first"
        )
        .copy()
    )


# ------------------------------------------------------------
# Verify uniqueness
# ------------------------------------------------------------

unique_grid_hours = (
    hourly_df[["grid_id", "timestamp"]]
    .drop_duplicates()
    .shape[0]
)

print()
print("Total records:")
print(len(hourly_df))

print()
print("Unique grid-hour combinations:")
print(unique_grid_hours)

if len(hourly_df) == unique_grid_hours:
    print()
    print("VALIDATION PASSED:")
    print("One record exists per grid and hour.")
else:
    print()
    print("WARNING:")
    print("Duplicate grid-hour records remain.")


# ------------------------------------------------------------
# Geometry is NOT included
# ------------------------------------------------------------

if "geometry" in hourly_df.columns:

    print()
    print(
        "Removing geometry from hourly analytics "
        "to avoid duplication."
    )

    hourly_df = hourly_df.drop(
        columns=["geometry"]
    )


# ------------------------------------------------------------
# Write hourly Parquet
# ------------------------------------------------------------

clean_output(HOURLY_PARQUET_DIR)
HOURLY_PARQUET_DIR.mkdir(
    parents=True,
    exist_ok=True
)

hourly_parquet_file = (
    HOURLY_PARQUET_DIR
    / "hourly_grid_summary.parquet"
)

hourly_df.to_parquet(
    hourly_parquet_file,
    engine="pyarrow",
    index=False
)

print()
print("Hourly grid summary Parquet created:")
print(hourly_parquet_file)

print()
print("Hourly Parquet size:")
print(format_size(folder_size_bytes(hourly_parquet_file)))


# ============================================================
# STATIC GRID REFERENCE
# ============================================================

print()
print("=" * 70)
print("STATIC GRID REFERENCE")
print("=" * 70)

if GEOJSON_FILE is not None:

    target_geojson = (
        STATIC_GRID_DIR / "milano-grid.geojson"
    )

    shutil.copy2(
        GEOJSON_FILE,
        target_geojson
    )

    print()
    print("Static grid reference copied to:")
    print(target_geojson)

    print()
    print("Geometry remains only in the static grid reference.")

else:

    print()
    print("WARNING:")
    print("milano-grid.geojson was not found.")
    print(
        "Place it under data/reference/ and run SP6 again."
    )


# ============================================================
# STEP 4
# DASHBOARD SUMMARY CSV
# ============================================================

print()
print("=" * 70)
print("STEP 4 - CREATE DASHBOARD SUMMARY CSV")
print("=" * 70)


# ------------------------------------------------------------
# Calculate summary metrics
# ------------------------------------------------------------

total_records = len(hourly_df)

unique_grids = hourly_df["grid_id"].nunique()

unique_hours = hourly_df["timestamp"].nunique()

total_activity = hourly_df["total_activity"].sum()

average_activity = hourly_df["total_activity"].mean()

maximum_activity = hourly_df["total_activity"].max()

minimum_activity = hourly_df["total_activity"].min()


# Date range
min_timestamp = hourly_df["timestamp"].min()
max_timestamp = hourly_df["timestamp"].max()


# Highest activity grid
grid_activity = (
    hourly_df
    .groupby("grid_id", as_index=False)["total_activity"]
    .sum()
    .sort_values(
        "total_activity",
        ascending=False
    )
)

top_grid_id = int(
    grid_activity.iloc[0]["grid_id"]
)

top_grid_activity = float(
    grid_activity.iloc[0]["total_activity"]
)


# ------------------------------------------------------------
# Dashboard summary DataFrame
# ------------------------------------------------------------

dashboard_summary = pd.DataFrame(
    {
        "metric": [
            "total_hourly_records",
            "unique_grids",
            "unique_hours",
            "total_activity",
            "average_activity",
            "maximum_hourly_activity",
            "minimum_hourly_activity",
            "top_activity_grid_id",
            "top_activity_grid_total",
            "start_timestamp",
            "end_timestamp",
        ],
        "value": [
            total_records,
            unique_grids,
            unique_hours,
            total_activity,
            average_activity,
            maximum_activity,
            minimum_activity,
            top_grid_id,
            top_grid_activity,
            str(min_timestamp),
            str(max_timestamp),
        ],
    }
)


dashboard_csv = (
    DASHBOARD_DIR / "dashboard_summary.csv"
)

dashboard_summary.to_csv(
    dashboard_csv,
    index=False
)

print()
print("Dashboard summary:")
print(dashboard_summary.to_string(index=False))

print()
print("Dashboard CSV created:")
print(dashboard_csv)


# ============================================================
# STEP 5
# READ PARQUET BACK AND VALIDATE
# ============================================================

print()
print("=" * 70)
print("STEP 5 - READ PARQUET BACK AND VALIDATE")
print("=" * 70)


# ------------------------------------------------------------
# Read clean Parquet
# ------------------------------------------------------------

print()
print("Reading clean activity Parquet...")

clean_readback = pd.read_parquet(
    clean_parquet_file,
    engine="pyarrow"
)

print()
print("Clean Parquet rows:")
print(len(clean_readback))

print()
print("Clean Parquet schema:")
print(clean_readback.dtypes)


# ------------------------------------------------------------
# Validate clean count
# ------------------------------------------------------------

clean_count_valid = (
    len(clean_readback) == len(activity_df)
)

print()
print("Clean Parquet count validation:")
print(clean_count_valid)


# ------------------------------------------------------------
# Read hourly Parquet
# ------------------------------------------------------------

print()
print("Reading hourly_grid_summary Parquet...")

hourly_readback = pd.read_parquet(
    hourly_parquet_file,
    engine="pyarrow"
)

print()
print("Hourly Parquet rows:")
print(len(hourly_readback))

print()
print("Hourly Parquet schema:")
print(hourly_readback.dtypes)


# ------------------------------------------------------------
# Validate hourly count
# ------------------------------------------------------------

hourly_count_valid = (
    len(hourly_readback) == len(hourly_df)
)

print()
print("Hourly Parquet count validation:")
print(hourly_count_valid)


# ------------------------------------------------------------
# Validate hourly uniqueness
# ------------------------------------------------------------

readback_duplicate_count = (
    hourly_readback
    .duplicated(
        subset=["grid_id", "timestamp"]
    )
    .sum()
)

print()
print("Duplicate grid-hour records after readback:")
print(readback_duplicate_count)


# ------------------------------------------------------------
# Validate geometry separation
# ------------------------------------------------------------

geometry_in_hourly = (
    "geometry" in hourly_readback.columns
)

print()
print("Geometry present in hourly analytics:")
print(geometry_in_hourly)

if not geometry_in_hourly:
    print(
        "VALIDATION PASSED: geometry is kept "
        "separately in the static reference."
    )


# ============================================================
# STEP 6
# FILE SIZE COMPARISON
# ============================================================

print()
print("=" * 70)
print("STEP 6 - FILE SIZE COMPARISON")
print("=" * 70)


# ------------------------------------------------------------
# CSV sizes
# ------------------------------------------------------------

sp4_csv_size = folder_size_bytes(SP4_FILE)

sp3_csv_size = folder_size_bytes(SP3_FILE)

dashboard_csv_size = folder_size_bytes(DASHBOARD_DIR)


# ------------------------------------------------------------
# Parquet sizes
# ------------------------------------------------------------

clean_parquet_size = folder_size_bytes(
    clean_parquet_file
)

partitioned_parquet_size = folder_size_bytes(
    partitioned_activity_dir
)

hourly_parquet_size = folder_size_bytes(
    hourly_parquet_file
)


print()
print("FILE SIZE COMPARISON")
print("-" * 70)

print(
    "SP4 enriched CSV:              ",
    format_size(sp4_csv_size)
)

print(
    "SP3 hourly CSV:                ",
    format_size(sp3_csv_size)
)

print(
    "Clean activity Parquet:        ",
    format_size(clean_parquet_size)
)

print(
    "Partitioned activity Parquet:  ",
    format_size(partitioned_parquet_size)
)

print(
    "Hourly summary Parquet:        ",
    format_size(hourly_parquet_size)
)

print(
    "Dashboard summary CSV:         ",
    format_size(dashboard_csv_size)
)


# ------------------------------------------------------------
# Calculate compression ratio
# ------------------------------------------------------------

if clean_parquet_size > 0:

    clean_ratio = (
        sp4_csv_size / clean_parquet_size
    )

else:
    clean_ratio = 0


if hourly_parquet_size > 0:

    hourly_ratio = (
        sp3_csv_size / hourly_parquet_size
    )

else:
    hourly_ratio = 0


print()
print("Approximate CSV / Parquet size ratios:")

print(
    "SP4 CSV vs clean Parquet:",
    round(clean_ratio, 2),
    "x"
)

print(
    "SP3 CSV vs hourly Parquet:",
    round(hourly_ratio, 2),
    "x"
)


# ============================================================
# COLUMNAR STORAGE EXPLANATION
# ============================================================

print()
print("=" * 70)
print("COLUMNAR STORAGE OBSERVATION")
print("=" * 70)

print(
    """
Parquet provides several advantages over CSV:

1. Columnar storage
   Data is stored by column instead of row, allowing Spark,
   Pandas and other engines to read only the required columns.

2. Compression
   Similar values stored together compress efficiently,
   reducing disk usage.

3. Schema preservation
   Parquet stores data types such as integer, double and
   timestamp instead of treating everything as text.

4. Faster analytical queries
   Queries that use only a few columns can avoid reading
   unnecessary data.

5. Partition pruning
   The cleaned activity data is partitioned by date,
   allowing processing engines to read only the required
   date partitions.

6. Less geometry duplication
   Polygon geometry is kept in the static grid reference
   instead of being repeated in every hourly analytics row.
"""
)


# ============================================================
# BUILD VALIDATION REPORT
# ============================================================

validation_report = {
    "project": "NP1",
    "phase": "SP6",
    "spark_hadoop_workaround": (
        "Parquet storage performed with Pandas + PyArrow "
        "to avoid Windows Hadoop winutils dependency."
    ),

    "inputs": {
        "sp4_file": str(SP4_FILE),
        "sp3_file": str(SP3_FILE),
        "geojson_file": (
            str(GEOJSON_FILE)
            if GEOJSON_FILE is not None
            else None
        ),
    },

    "clean_activity": {
        "input_rows": int(before_cleaning),
        "clean_rows": int(len(activity_df)),
        "parquet_file": str(clean_parquet_file),
        "parquet_size_bytes": int(clean_parquet_size),
        "parquet_size": format_size(clean_parquet_size),
        "readback_rows": int(len(clean_readback)),
        "count_validation": bool(clean_count_valid),
    },

    "partitioning": {
        "partition_column": "date",
        "partition_count": int(len(partition_folders)),
        "output_directory": str(partitioned_activity_dir),
        "total_size_bytes": int(partitioned_parquet_size),
        "total_size": format_size(partitioned_parquet_size),
    },

    "hourly_grid_summary": {
        "rows": int(len(hourly_df)),
        "unique_grid_hour_records": int(unique_grid_hours),
        "duplicate_records": int(duplicate_count),
        "readback_rows": int(len(hourly_readback)),
        "count_validation": bool(hourly_count_valid),
        "readback_duplicates": int(readback_duplicate_count),
        "geometry_in_hourly": bool(geometry_in_hourly),
        "parquet_file": str(hourly_parquet_file),
        "parquet_size_bytes": int(hourly_parquet_size),
        "parquet_size": format_size(hourly_parquet_size),
    },

    "static_grid_reference": {
        "geojson_file": (
            str(STATIC_GRID_DIR / "milano-grid.geojson")
            if GEOJSON_FILE is not None
            else None
        ),
        "geometry_separated": not geometry_in_hourly,
    },

    "dashboard": {
        "csv_file": str(dashboard_csv),
    },

    "file_size_comparison": {
        "sp4_csv_bytes": int(sp4_csv_size),
        "clean_parquet_bytes": int(clean_parquet_size),
        "sp3_csv_bytes": int(sp3_csv_size),
        "hourly_parquet_bytes": int(hourly_parquet_size),
        "sp4_csv_to_parquet_ratio": round(
            clean_ratio,
            3
        ),
        "sp3_csv_to_parquet_ratio": round(
            hourly_ratio,
            3
        ),
    },

    "performance_observations": [
        {
            "observation": "Parquet uses columnar storage.",
            "evidence": (
                "Parquet preserves schema and allows "
                "column-level reads."
            ),
        },
        {
            "observation": "Partitioning by date supports partition pruning.",
            "evidence": (
                f"{len(partition_folders)} date partition(s) "
                "were created."
            ),
        },
        {
            "observation": "Static geometry avoids duplication.",
            "evidence": (
                "Geometry is excluded from hourly analytics "
                "and retained in the static GeoJSON reference."
            ),
        },
    ],
}


# ============================================================
# SAVE VALIDATION REPORT
# ============================================================

with open(
    REPORT_FILE,
    "w",
    encoding="utf-8"
) as file:

    json.dump(
        validation_report,
        file,
        indent=4,
        default=str
    )


# ============================================================
# FINAL OUTPUT SUMMARY
# ============================================================

print()
print("=" * 70)
print("SP6 OUTPUTS")
print("=" * 70)

print()
print("1. Clean activity Parquet:")
print(clean_parquet_file)

print()
print("2. Date-partitioned clean activity:")
print(partitioned_activity_dir)

print()
print("3. Hourly grid summary Parquet:")
print(hourly_parquet_file)

print()
print("4. Static grid reference:")
print(
    STATIC_GRID_DIR / "milano-grid.geojson"
    if GEOJSON_FILE is not None
    else "NOT FOUND"
)

print()
print("5. Dashboard summary CSV:")
print(dashboard_csv)

print()
print("6. Validation report:")
print(REPORT_FILE)


# ============================================================
# FINAL VALIDATION
# ============================================================

all_valid = (
    clean_count_valid
    and hourly_count_valid
    and readback_duplicate_count == 0
    and not geometry_in_hourly
)

print()
print("=" * 70)

if all_valid:
    print("ALL SP6 VALIDATIONS PASSED")
else:
    print("SP6 COMPLETED WITH VALIDATION WARNINGS")

print("=" * 70)

print()
print("SP6 completed successfully.")

