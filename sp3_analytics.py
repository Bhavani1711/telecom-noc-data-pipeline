import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    max as spark_max,
    desc,
    hour,
    to_date,
    lit,
    round as spark_round
)
from pyspark.sql.types import (
    NumericType,
    StringType,
    TimestampType
)


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_DIR = r"C:\Users\bhavani.as\Desktop\NP1"

OUTPUT_DIR = os.path.join(PROJECT_DIR, "sp3_output")

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("NP1 SP3 Telecom Analytics")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("SP3 TELECOM ANALYTICS")
print("=" * 70)

print("Spark version:", spark.version)
print("Project directory:", PROJECT_DIR)
print()


# ============================================================
# FIND SP2 OUTPUT
# ============================================================

print("=" * 70)
print("SEARCHING FOR SP2 OUTPUT")
print("=" * 70)


# Possible names for the SP2 consolidated output
possible_patterns = [
    "**/grid_hourly_analytics.csv",
    "**/grid_hourly_summary.csv",
    "**/consolidated_grid_hourly.csv",
    "**/grid_summary.csv",
    "**/hourly_grid_summary.csv"
]


sp2_files = []

for pattern in possible_patterns:
    search_pattern = os.path.join(PROJECT_DIR, pattern)
    matches = glob.glob(search_pattern, recursive=True)

    for match in matches:
        if os.path.isfile(match) and match not in sp2_files:
            sp2_files.append(match)


if not sp2_files:

    print()
    print("ERROR: No SP2 output CSV was found.")
    print()
    print("The script searched for:")
    
    for pattern in possible_patterns:
        print("   ", pattern)

    print()
    print("Make sure your SP2 output CSV is somewhere inside:")
    print(PROJECT_DIR)

    spark.stop()
    raise SystemExit(1)


print()
print("SP2 OUTPUT FILE(S) FOUND:")
print()

for i, file in enumerate(sp2_files, start=1):
    print(f"{i}. {file}")


# ============================================================
# SELECT SP2 OUTPUT
# ============================================================

# Prefer grid_hourly_analytics.csv if it exists
preferred_file = None

for file in sp2_files:
    if os.path.basename(file).lower() == "grid_hourly_analytics.csv":
        preferred_file = file
        break

if preferred_file is None:
    preferred_file = sp2_files[0]


INPUT_FILE = preferred_file

print()
print("Using SP2 output:")
print(INPUT_FILE)


# ============================================================
# READ SP2 DATA
# ============================================================

print()
print("=" * 70)
print("READING SP2 DATA")
print("=" * 70)


df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(INPUT_FILE)
)


# ============================================================
# DISPLAY RAW SP2 STRUCTURE
# ============================================================

print()
print("SP2 columns:")
print(df.columns)

print()
print("SP2 schema:")
df.printSchema()

print()
print("SP2 row count:", df.count())


# ============================================================
# NORMALIZE COLUMN NAMES
# ============================================================

df = df.toDF(
    *[
        c.strip().lower().replace(" ", "_")
        for c in df.columns
    ]
)


print()
print("Normalized columns:")
print(df.columns)


# ============================================================
# REQUIRED COLUMNS
# ============================================================

required_columns = [
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet"
]


missing_columns = [
    c for c in required_columns
    if c not in df.columns
]


if missing_columns:

    print()
    print("=" * 70)
    print("ERROR: REQUIRED COLUMNS ARE MISSING")
    print("=" * 70)

    print()
    print("Missing columns:")
    for c in missing_columns:
        print("   ", c)

    print()
    print("Columns actually found:")
    for c in df.columns:
        print("   ", c)

    print()
    print("SP3 cannot continue safely.")

    spark.stop()
    raise SystemExit(1)


# ============================================================
# CONVERT DATA TYPES SAFELY
# ============================================================

df = (
    df
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
    .withColumn("grid_id", col("grid_id").cast("string"))
    .withColumn("sms_in", col("sms_in").cast("double"))
    .withColumn("sms_out", col("sms_out").cast("double"))
    .withColumn("call_in", col("call_in").cast("double"))
    .withColumn("call_out", col("call_out").cast("double"))
    .withColumn("internet", col("internet").cast("double"))
)


# ============================================================
# CHECK NULLS
# ============================================================

print()
print("=" * 70)
print("DATA QUALITY CHECK")
print("=" * 70)


for column_name in required_columns:

    null_count = df.filter(
        col(column_name).isNull()
    ).count()

    print(
        f"{column_name:15s} -> "
        f"{null_count} null values"
    )


# ============================================================
# SP3 QUESTION 1
#
# Collapse country-code records into:
# one row per timestamp + grid_id
#
# SUM:
# sms_in
# sms_out
# call_in
# call_out
# internet
# ============================================================

print()
print("=" * 70)
print("STEP 1: CONSOLIDATING COUNTRY-CODE RECORDS")
print("=" * 70)


consolidated_grid_hour = (
    df
    .groupBy(
        "timestamp",
        "grid_id"
    )
    .agg(
        spark_sum("sms_in").alias("sms_in"),
        spark_sum("sms_out").alias("sms_out"),
        spark_sum("call_in").alias("call_in"),
        spark_sum("call_out").alias("call_out"),
        spark_sum("internet").alias("internet")
    )
    .orderBy(
        "timestamp",
        "grid_id"
    )
)


print()
print("Consolidated DataFrame created.")

print()
print("Rows after consolidation:")
print(consolidated_grid_hour.count())

print()
print("Sample consolidated records:")
consolidated_grid_hour.show(
    10,
    truncate=False
)


# ============================================================
# VERIFY ONE RECORD PER GRID + TIMESTAMP
# ============================================================

duplicate_groups = (
    consolidated_grid_hour
    .groupBy(
        "timestamp",
        "grid_id"
    )
    .count()
    .filter(col("count") > 1)
)


duplicate_count = duplicate_groups.count()


if duplicate_count > 0:

    print()
    print("ERROR: Duplicate timestamp + grid_id records found.")
    print("Duplicate groups:", duplicate_count)

    spark.stop()
    raise SystemExit(1)

else:

    print()
    print("PASS: Exactly one record per timestamp + grid_id.")


# ============================================================
# SP3 QUESTION 2
#
# TOTAL SMS
# TOTAL CALL
# INTERNET
# TOTAL ACTIVITY
# DAILY ACTIVITY PER GRID
# ============================================================

print()
print("=" * 70)
print("STEP 2: DERIVING ACTIVITY FEATURES")
print("=" * 70)


hourly_grid = (
    consolidated_grid_hour

    # SMS activity
    .withColumn(
        "total_sms_activity",
        col("sms_in") + col("sms_out")
    )

    # Call activity
    .withColumn(
        "total_call_activity",
        col("call_in") + col("call_out")
    )

    # Total activity
    .withColumn(
        "total_activity",
        col("total_sms_activity")
        + col("total_call_activity")
        + col("internet")
    )

    # Date
    .withColumn(
        "date",
        to_date(col("timestamp"))
    )
)


# Daily activity per grid
daily_grid_activity = (
    hourly_grid
    .groupBy(
        "grid_id",
        "date"
    )
    .agg(
        spark_sum("total_activity").alias(
            "daily_activity"
        )
    )
)


# Join daily activity back to hourly data
hourly_grid = (
    hourly_grid
    .join(
        daily_grid_activity,
        on=["grid_id", "date"],
        how="left"
    )
)


print()
print("Activity features created.")

hourly_grid.select(
    "timestamp",
    "grid_id",
    "total_sms_activity",
    "total_call_activity",
    "internet",
    "total_activity",
    "daily_activity"
).show(
    10,
    truncate=False
)


# ============================================================
# SP3 QUESTION 3
#
# TOP TEN HIGH-ACTIVITY GRIDS
# ============================================================

print()
print("=" * 70)
print("STEP 3: TOP 10 HIGH-ACTIVITY GRIDS")
print("=" * 70)


top10_grids = (
    hourly_grid
    .groupBy("grid_id")
    .agg(
        spark_sum("total_activity").alias(
            "total_grid_activity"
        )
    )
    .orderBy(
        desc("total_grid_activity")
    )
    .limit(10)
)


print()
print("Top 10 high-activity grids:")

top10_grids.show(
    10,
    truncate=False
)


# Save top 10 grids
top10_output = os.path.join(
    OUTPUT_DIR,
    "top10_high_activity_grids"
)

(
    top10_grids
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(top10_output)
)


# ============================================================
# SP3 QUESTION 4
#
# PEAK ACTIVITY HOUR
# ============================================================

print()
print("=" * 70)
print("STEP 4: PEAK ACTIVITY HOUR")
print("=" * 70)


hourly_activity = (
    hourly_grid
    .withColumn(
        "hour",
        hour("timestamp")
    )
    .groupBy("hour")
    .agg(
        spark_sum("total_activity").alias(
            "activity"
        )
    )
    .orderBy(
        desc("activity")
    )
)


print()
print("Activity by hour:")

hourly_activity.show(
    24,
    truncate=False
)


peak_hour = hourly_activity.first()


if peak_hour is not None:

    print()
    print(
        "PEAK ACTIVITY HOUR:",
        peak_hour["hour"]
    )

    print(
        "Peak activity:",
        peak_hour["activity"]
    )


# Save hourly activity
hourly_activity_output = os.path.join(
    OUTPUT_DIR,
    "activity_by_hour"
)

(
    hourly_activity
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(hourly_activity_output)
)


# ============================================================
# SP3 QUESTION 5
#
# INTERNET SHARE OF TOTAL ACTIVITY
# ============================================================

print()
print("=" * 70)
print("STEP 5: INTERNET SHARE")
print("=" * 70)


hourly_grid = (
    hourly_grid
    .withColumn(
        "internet_share",
        spark_round(
            col("internet")
            / col("total_activity"),
            4
        )
    )
)


# Avoid division by zero
hourly_grid = hourly_grid.withColumn(
    "internet_share",
    col("internet_share")
)


print()
print("Internet share calculated.")

hourly_grid.select(
    "timestamp",
    "grid_id",
    "internet",
    "total_activity",
    "internet_share"
).show(
    10,
    truncate=False
)


# ============================================================
# SP3 QUESTION 6
#
# CANONICAL DOWNSTREAM DATAFRAME
#
# hourly_grid_summary
#
# EXACTLY ONE RECORD PER
# grid_id + hourly timestamp
# ============================================================

print()
print("=" * 70)
print("STEP 6: BUILDING hourly_grid_summary")
print("=" * 70)


hourly_grid_summary = (
    hourly_grid
    .select(
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
        "internet_share"
    )
    .orderBy(
        "timestamp",
        "grid_id"
    )
)


# ============================================================
# FINAL VALIDATION
# ============================================================

print()
print("=" * 70)
print("FINAL VALIDATION")
print("=" * 70)


final_rows = hourly_grid_summary.count()


unique_grid_timestamp = (
    hourly_grid_summary
    .select(
        "timestamp",
        "grid_id"
    )
    .distinct()
    .count()
)


print()
print("Final rows:", final_rows)
print(
    "Unique timestamp + grid_id:",
    unique_grid_timestamp
)


if final_rows != unique_grid_timestamp:

    print()
    print("ERROR: hourly_grid_summary contains duplicates.")

    spark.stop()
    raise SystemExit(1)


print()
print(
    "PASS: hourly_grid_summary has exactly "
    "one record per grid_id + timestamp."
)


# ============================================================
# FINAL SCHEMA
# ============================================================

print()
print("Final hourly_grid_summary schema:")

hourly_grid_summary.printSchema()


print()
print("Final sample:")

hourly_grid_summary.show(
    20,
    truncate=False
)


# ============================================================
# SAVE CANONICAL OUTPUT
# ============================================================

canonical_output = os.path.join(
    OUTPUT_DIR,
    "hourly_grid_summary"
)


(
    hourly_grid_summary
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(canonical_output)
)


print()
print("=" * 70)
print("SP3 COMPLETED SUCCESSFULLY")
print("=" * 70)

print()
print("Canonical DataFrame:")
print("hourly_grid_summary")

print()
print("Output location:")
print(canonical_output)

print()
print("Rows:", final_rows)

print()
print("SP3 output files:")
print("1. hourly_grid_summary")
print("2. top10_high_activity_grids")
print("3. activity_by_hour")


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()