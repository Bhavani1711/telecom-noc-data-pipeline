
import os
import sys
import glob
import json
import shutil
import logging
import argparse
from datetime import datetime

# ============================================================
# WINDOWS PYSPARK CONFIGURATION
# ============================================================
# Make Python available to Spark
python_exe = sys.executable
os.environ["PYSPARK_PYTHON"] = python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

# ------------------------------------------------------------
# IMPORTANT:
# Spark on Windows needs Hadoop's winutils.exe when writing
# Parquet/local files.
#
# This script looks for a local Hadoop installation.
# ------------------------------------------------------------
possible_hadoop_paths = [
    os.path.join(os.getcwd(), "hadoop"),
    os.path.join(os.path.dirname(os.getcwd()), "hadoop"),
    r"C:\hadoop",
    r"C:\Users\bhavani.as\hadoop"
]

hadoop_home = None
winutils_path = None

for path in possible_hadoop_paths:
    candidate = os.path.join(path, "bin", "winutils.exe")

    if os.path.isfile(candidate):
        hadoop_home = path
        winutils_path = candidate
        break

if hadoop_home:
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["hadoop.home.dir"] = hadoop_home
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ["PATH"]

else:
    # Give Spark a harmless existing directory so that the error
    # message is clear if winutils is genuinely unavailable.
    fallback_hadoop = os.path.join(os.getcwd(), "hadoop")

    os.environ["HADOOP_HOME"] = fallback_hadoop
    os.environ["hadoop.home.dir"] = fallback_hadoop

# ============================================================
# SPARK IMPORTS
# ============================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    dayofweek,
    coalesce,
    lit,
    sum as spark_sum,
    avg as spark_avg,
    count
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType
)

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("telecom_pipeline")


# ============================================================
# SPARK SESSION
# ============================================================
def create_spark():

    logger.info("Creating Spark session")

    spark = (
        SparkSession.builder
        .appName("Telecom-SP7-Pipeline")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.hadoop.fs.file.impl",
                "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark version: %s", spark.version)

    return spark


# ============================================================
# READ RAW
# ============================================================
def read_raw(spark, input_path):

    logger.info("=" * 70)
    logger.info("READING RAW INPUT")
    logger.info("=" * 70)

    if not os.path.exists(input_path):
        raise FileNotFoundError(
            f"Input path does not exist: {input_path}"
        )

    csv_files = sorted(
        glob.glob(os.path.join(input_path, "*.csv"))
    )

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV input files found in: {input_path}"
        )

    logger.info("Input files found: %d", len(csv_files))

    for file in csv_files:
        logger.info("Input file: %s", file)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_files)
    )

    input_rows = df.count()

    logger.info("Input rows: %d", input_rows)
    logger.info("Raw columns: %s", df.columns)

    return df


# ============================================================
# CLEAN
# ============================================================
def clean(df):

    logger.info("=" * 70)
    logger.info("CLEANING DATA")
    logger.info("=" * 70)

    # --------------------------------------------------------
    # Rename original columns
    # --------------------------------------------------------
    rename_map = {
        "datetime": "timestamp",
        "CellID": "grid_id",
        "countrycode": "country_code",
        "smsin": "sms_in",
        "smsout": "sms_out",
        "callin": "call_in",
        "callout": "call_out",
        "internet": "internet_activity"
    }

    for old_name, new_name in rename_map.items():

        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    # --------------------------------------------------------
    # Validate required columns
    # --------------------------------------------------------
    required_columns = [
        "timestamp",
        "grid_id",
        "country_code",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    missing = [
        c for c in required_columns
        if c not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Missing required columns: {missing}"
        )

    before_count = df.count()

    # --------------------------------------------------------
    # Count nulls before cleaning
    # --------------------------------------------------------
    null_expression = sum(
        df[c].isNull().cast("long")
        for c in required_columns
    )

    null_count = df.select(
        spark_sum(null_expression).alias("null_count")
    ).collect()[0]["null_count"]

    null_count = int(null_count or 0)

    logger.info("Null values detected: %d", null_count)

    # --------------------------------------------------------
    # Timestamp conversion
    # --------------------------------------------------------
    df = df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )

    # --------------------------------------------------------
    # Numeric columns
    # --------------------------------------------------------
    numeric_columns = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity"
    ]

    for c in numeric_columns:

        df = df.withColumn(
            c,
            col(c).cast(DoubleType())
        )

        # Missing activity is treated as zero
        df = df.withColumn(
            c,
            coalesce(col(c), lit(0.0))
        )

    # --------------------------------------------------------
    # Required identifiers
    # --------------------------------------------------------
    df = df.withColumn(
        "grid_id",
        col("grid_id").cast(IntegerType())
    )

    df = df.withColumn(
        "country_code",
        col("country_code").cast(StringType())
    )

    # --------------------------------------------------------
    # Remove rows with invalid timestamp/grid
    # --------------------------------------------------------
    df = df.filter(
        col("timestamp").isNotNull()
        & col("grid_id").isNotNull()
    )

    # --------------------------------------------------------
    # Derived activity
    # --------------------------------------------------------
    df = df.withColumn(
        "total_activity",
        col("sms_in")
        + col("sms_out")
        + col("call_in")
        + col("call_out")
        + col("internet_activity")
    )

    # --------------------------------------------------------
    # Time features
    # --------------------------------------------------------
    df = df.withColumn(
        "date",
        to_date(col("timestamp"))
    )

    df = df.withColumn(
        "hour",
        hour(col("timestamp"))
    )

    df = df.withColumn(
        "day_of_week",
        dayofweek(col("timestamp"))
    )

    after_count = df.count()

    rejected = before_count - after_count

    logger.info("Rows before cleaning: %d", before_count)
    logger.info("Rows after cleaning: %d", after_count)
    logger.info("Rejected rows: %d", rejected)
    logger.info("Cleaned columns: %s", df.columns)

    return df


# ============================================================
# AGGREGATE
# ============================================================
def aggregate(df):

    logger.info("=" * 70)
    logger.info("CREATING HOURLY GRID SUMMARY")
    logger.info("=" * 70)

    summary = (
        df.groupBy(
            "grid_id",
            "date",
            "hour"
        )
        .agg(
            spark_sum("sms_in").alias("sms_in"),
            spark_sum("sms_out").alias("sms_out"),
            spark_sum("call_in").alias("call_in"),
            spark_sum("call_out").alias("call_out"),
            spark_sum("internet_activity").alias(
                "internet_activity"
            ),
            spark_sum("total_activity").alias(
                "total_activity"
            ),
            count("*").alias("record_count")
        )
    )

    summary_rows = summary.count()

    logger.info(
        "Hourly grid summary rows: %d",
        summary_rows
    )

    return summary


# ============================================================
# ENRICH
# ============================================================
def enrich(summary, reference_path):

    logger.info("=" * 70)
    logger.info("ENRICHING DATA")
    logger.info("=" * 70)

    # --------------------------------------------------------
    # Geometry intentionally NOT joined into analytics data.
    #
    # The full Polygon geometry remains in the GeoJSON
    # reference file for map rendering.
    # --------------------------------------------------------
    if reference_path:

        if os.path.exists(reference_path):

            logger.info(
                "Static grid reference found: %s",
                reference_path
            )

            logger.info(
                "Polygon geometry will remain in the reference GeoJSON."
            )

        else:

            logger.warning(
                "Reference GeoJSON not found: %s",
                reference_path
            )

    # --------------------------------------------------------
    # Simple analytical enrichment
    # --------------------------------------------------------
    enriched = (
        summary
        .withColumn(
            "avg_activity_per_record",
            coalesce(
                col("total_activity")
                / col("record_count"),
                lit(0.0)
            )
        )
    )

    return enriched


# ============================================================
# WRITE OUTPUTS
# ============================================================
def write_outputs(cleaned, hourly_summary, output_path):

    logger.info("=" * 70)
    logger.info("WRITING OUTPUTS")
    logger.info("=" * 70)

    os.makedirs(output_path, exist_ok=True)

    # --------------------------------------------------------
    # Output directories
    # --------------------------------------------------------
    cleaned_path = os.path.join(
        output_path,
        "cleaned_activity"
    )

    summary_path = os.path.join(
        output_path,
        "hourly_grid_summary"
    )

    dashboard_path = os.path.join(
        output_path,
        "dashboard_summary.csv"
    )

    # --------------------------------------------------------
    # Remove previous incomplete output
    # --------------------------------------------------------
    for path in [
        cleaned_path,
        summary_path
    ]:

        if os.path.exists(path):

            logger.info(
                "Removing previous output: %s",
                path
            )

            shutil.rmtree(path, ignore_errors=True)

    # --------------------------------------------------------
    # 1 + 2. CLEANED ACTIVITY -> PARQUET
    # Partition by date
    # --------------------------------------------------------
    logger.info(
        "Writing cleaned activity Parquet: %s",
        cleaned_path
    )

    (
        cleaned
        .repartition("date")
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(cleaned_path)
    )

    logger.info("Cleaned activity Parquet written successfully.")

    # --------------------------------------------------------
    # 3. HOURLY GRID SUMMARY -> PARQUET
    # No geometry duplicated here.
    # --------------------------------------------------------
    logger.info(
        "Writing hourly grid summary Parquet: %s",
        summary_path
    )

    (
        hourly_summary
        .repartition("date")
        .write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(summary_path)
    )

    logger.info(
        "Hourly grid summary Parquet written successfully."
    )

    # --------------------------------------------------------
    # 4. DASHBOARD SUMMARY -> CSV
    # --------------------------------------------------------
    logger.info(
        "Creating dashboard summary: %s",
        dashboard_path
    )

    dashboard = (
        hourly_summary
        .groupBy("date")
        .agg(
            spark_sum("total_activity").alias(
                "total_activity"
            ),
            spark_avg("total_activity").alias(
                "avg_hourly_activity"
            ),
            count("*").alias(
                "grid_hour_records"
            )
        )
        .orderBy("date")
    )

    # coalesce(1) makes a small easy-to-inspect CSV
    dashboard.coalesce(1).write.mode(
        "overwrite"
    ).option(
        "header", True
    ).csv(
        dashboard_path
    )

    logger.info("Dashboard CSV written successfully.")

    logger.info("=" * 70)
    logger.info("ALL OUTPUTS WRITTEN SUCCESSFULLY")
    logger.info("=" * 70)


# ============================================================
# MAIN
# ============================================================
def main():

    parser = argparse.ArgumentParser(
        description="Telecom Spark SP7 Pipeline"
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Directory containing raw CSV files"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output directory"
    )

    parser.add_argument(
        "--reference",
        required=True,
        help="Path to milano-grid.geojson"
    )

    args = parser.parse_args()

    start_time = datetime.now()

    logger.info("=" * 70)
    logger.info("TELECOM SPARK PIPELINE STARTED")
    logger.info("Start time: %s", start_time)
    logger.info("=" * 70)

    spark = None

    try:

        # ----------------------------------------------------
        # Check input before starting Spark
        # ----------------------------------------------------
        if not os.path.exists(args.input):

            raise FileNotFoundError(
                f"Input path does not exist: {args.input}"
            )

        csv_files = glob.glob(
            os.path.join(args.input, "*.csv")
        )

        if not csv_files:

            raise FileNotFoundError(
                f"No CSV files found in input directory: "
                f"{args.input}"
            )

        # ----------------------------------------------------
        # Check reference
        # ----------------------------------------------------
        if not os.path.exists(args.reference):

            raise FileNotFoundError(
                f"Reference GeoJSON not found: "
                f"{args.reference}"
            )

        # ----------------------------------------------------
        # Create Spark
        # ----------------------------------------------------
        spark = create_spark()

        # ----------------------------------------------------
        # Pipeline
        # ----------------------------------------------------
        raw = read_raw(
            spark,
            args.input
        )

        cleaned = clean(raw)

        hourly_summary = aggregate(
            cleaned
        )

        enriched = enrich(
            hourly_summary,
            args.reference
        )

        write_outputs(
            cleaned,
            enriched,
            args.output
        )

        # ----------------------------------------------------
        # Final status
        # ----------------------------------------------------
        end_time = datetime.now()

        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info("Start time: %s", start_time)
        logger.info("End time: %s", end_time)
        logger.info("Final status: SUCCESS")
        logger.info("=" * 70)

    except Exception as e:

        end_time = datetime.now()

        logger.error("=" * 70)
        logger.error("PIPELINE FAILED")
        logger.error("=" * 70)
        logger.error("Error: %s", str(e))
        logger.error("Start time: %s", start_time)
        logger.error("End time: %s", end_time)
        logger.error("Final status: FAILED")
        logger.error("=" * 70)

        sys.exit(1)

    finally:

        if spark is not None:

            try:
                spark.stop()
            except Exception:
                pass


# ============================================================
# ENTRY POINT
# ============================================================
if __name__ == "__main__":
    main()
