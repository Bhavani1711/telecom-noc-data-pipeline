from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    input_file_name,
    to_timestamp,
    date_format,
    dayofweek,
    coalesce,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)

# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("NP1_SP2_Cleaning")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# PATHS
# ============================================================

input_path = (
    r"C:\Users\bhavani.as\Desktop\NP1\data\landing"
    r"\sms-call-internet-mi-*.csv"
)

clean_output = (
    r"C:\Users\bhavani.as\Desktop\NP1\data\clean"
)

rejected_output = (
    r"C:\Users\bhavani.as\Desktop\NP1\data\rejected"
)


# ============================================================
# SOURCE SCHEMA
# ============================================================

source_schema = StructType([
    StructField("datetime", StringType(), True),
    StructField("CellID", IntegerType(), True),
    StructField("countrycode", IntegerType(), True),
    StructField("smsin", DoubleType(), True),
    StructField("smsout", DoubleType(), True),
    StructField("callin", DoubleType(), True),
    StructField("callout", DoubleType(), True),
    StructField("internet", DoubleType(), True)
])


# ============================================================
# READ RAW DATA
# ============================================================

raw_df = (
    spark.read
    .option("header", "true")
    .schema(source_schema)
    .csv(input_path)
)

print("\n========== SP2 CLEANING ==========")

raw_count = raw_df.count()

print(f"Raw rows: {raw_count}")


# ============================================================
# STANDARDIZE COLUMN NAMES + PARSE TIMESTAMP
# ============================================================

df = (
    raw_df
    .withColumn(
        "timestamp",
        to_timestamp(col("datetime"))
    )
    .withColumnRenamed("CellID", "grid_id")
    .withColumnRenamed("countrycode", "country_code")
    .withColumnRenamed("smsin", "sms_in")
    .withColumnRenamed("smsout", "sms_out")
    .withColumnRenamed("callin", "call_in")
    .withColumnRenamed("callout", "call_out")
    .withColumnRenamed("internet", "internet_activity")
    .withColumn(
        "input_file_name",
        input_file_name()
    )
)


# ============================================================
# INVALID ROW CONDITIONS
# ============================================================

invalid_timestamp = col("timestamp").isNull()

invalid_grid = col("grid_id").isNull()

negative_activity = (
    (coalesce(col("sms_in"), lit(0.0)) < 0)
    |
    (coalesce(col("sms_out"), lit(0.0)) < 0)
    |
    (coalesce(col("call_in"), lit(0.0)) < 0)
    |
    (coalesce(col("call_out"), lit(0.0)) < 0)
    |
    (coalesce(col("internet_activity"), lit(0.0)) < 0)
)

invalid_condition = (
    invalid_timestamp
    | invalid_grid
    | negative_activity
)


# ============================================================
# REJECTED ROWS
# ============================================================

rejected_df = df.filter(invalid_condition)

rejected_count = rejected_df.count()


# ============================================================
# CLEAN ROWS
# ============================================================

clean_df = df.filter(~invalid_condition)


# ============================================================
# NULL ACTIVITY COUNT
# ============================================================

null_activity_condition = (
    col("sms_in").isNull()
    |
    col("sms_out").isNull()
    |
    col("call_in").isNull()
    |
    col("call_out").isNull()
    |
    col("internet_activity").isNull()
)

null_activity_rows = (
    clean_df
    .filter(null_activity_condition)
    .count()
)


# ============================================================
# REPLACE NULL ACTIVITY VALUES WITH ZERO
# ============================================================

clean_df = (
    clean_df
    .withColumn(
        "sms_in",
        coalesce(col("sms_in"), lit(0.0))
    )
    .withColumn(
        "sms_out",
        coalesce(col("sms_out"), lit(0.0))
    )
    .withColumn(
        "call_in",
        coalesce(col("call_in"), lit(0.0))
    )
    .withColumn(
        "call_out",
        coalesce(col("call_out"), lit(0.0))
    )
    .withColumn(
        "internet_activity",
        coalesce(col("internet_activity"), lit(0.0))
    )
)


# ============================================================
# DERIVED ACTIVITY FEATURES
# ============================================================

clean_df = (
    clean_df
    .withColumn(
        "total_sms",
        col("sms_in") + col("sms_out")
    )
    .withColumn(
        "total_calls",
        col("call_in") + col("call_out")
    )
    .withColumn(
        "total_activity",
        col("sms_in")
        + col("sms_out")
        + col("call_in")
        + col("call_out")
        + col("internet_activity")
    )
)


# ============================================================
# TIME FEATURES
# ============================================================

clean_df = (
    clean_df
    .withColumn(
        "date",
        date_format(
            col("timestamp"),
            "yyyy-MM-dd"
        )
    )
    .withColumn(
        "hour",
        date_format(
            col("timestamp"),
            "HH"
        )
    )
    .withColumn(
        "day_of_week",
        dayofweek(col("timestamp"))
    )
)


# ============================================================
# CLEAN COUNT
# ============================================================

clean_count = clean_df.count()


# ============================================================
# CLEANING REPORT
# ============================================================

print("\n========== CLEANING REPORT ==========")

print(f"Raw rows: {raw_count}")
print(f"Rejected rows: {rejected_count}")
print(f"Clean rows: {clean_count}")
print(f"Null activity rows handled: {null_activity_rows}")

expected_clean_count = raw_count - rejected_count

print("\nExpected relationship:")
print(
    f"Raw rows - Rejected rows = "
    f"{expected_clean_count}"
)

print(
    f"Clean rows = {clean_count}"
)


# ============================================================
# ROW COUNT VALIDATION
# ============================================================

if clean_count != expected_clean_count:

    print("\n========== VALIDATION FAILED ==========")

    print(f"Raw rows      : {raw_count}")
    print(f"Rejected rows : {rejected_count}")
    print(f"Expected clean: {expected_clean_count}")
    print(f"Actual clean  : {clean_count}")

    raise RuntimeError(
        "SP2 validation failed: "
        "raw != clean + rejected"
    )

print(
    "\nRow-count validation: PASSED"
)


# ============================================================
# CADENCE CHECK
# ============================================================

print("\n========== CADENCE CHECK ==========")

file_count = (
    clean_df
    .select("input_file_name")
    .distinct()
    .count()
)

timestamp_count = (
    clean_df
    .select("timestamp")
    .distinct()
    .count()
)

expected_timestamps = file_count * 24

print(f"Files loaded: {file_count}")
print(f"Distinct timestamps: {timestamp_count}")
print(f"Expected timestamps: {expected_timestamps}")

if timestamp_count == expected_timestamps:
    print("Cadence validation: PASSED")
else:
    print("Cadence validation: CHECK REQUIRED")


# ============================================================
# FINAL SCHEMA
# ============================================================

print("\n========== CLEAN NETWORK SCHEMA ==========")

clean_df.printSchema()


# ============================================================
# SAMPLE
# ============================================================

print("\n========== CLEAN DATA SAMPLE ==========")

clean_df.show(
    10,
    truncate=False
)


# ============================================================
# SAVE CLEAN DATA
# ============================================================

print("\nWriting clean data...")

(
    clean_df
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(clean_output)
)

print(
    f"Clean data written to:\n{clean_output}"
)


# ============================================================
# SAVE REJECTED DATA
# ============================================================

if rejected_count > 0:

    print("\nWriting rejected data...")

    (
        rejected_df
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(rejected_output)
    )

    print(
        f"Rejected data written to:\n"
        f"{rejected_output}"
    )

else:

    print("\nNo rejected rows.")


# ============================================================
# FINAL SUMMARY
# ============================================================

print("\n========== SP2 COMPLETE ==========")

print(f"Raw rows       : {raw_count}")
print(f"Rejected rows  : {rejected_count}")
print(f"Clean rows     : {clean_count}")
print(f"Clean output   : {clean_output}")
print(f"Rejected output: {rejected_output}")


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()