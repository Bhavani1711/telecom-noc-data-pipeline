
# ============================================================
# SP5 - SPARK PERFORMANCE OPTIMIZATION
# ============================================================
# Student Activities:
# 1. Run explain() on a hotspot aggregation and read the plan
# 2. Cache a reused cleaned DataFrame and compare timings
# 3. Repartition by date and observe partition counts
# 4. Demonstrate column pruning
# 5. Broadcast static grid lookup and compare plans
# 6. Demonstrate over-partitioning
# 7. Document three performance observations
#
# IMPORTANT FOR WINDOWS:
# This version DOES NOT use Spark .write.csv().
# Output files are written using Python's csv/json modules.
# Therefore HADOOP_HOME / winutils.exe is NOT required for
# saving the SP5 results.
# ============================================================

import os
import sys
import csv
import json
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    sum as spark_sum,
    broadcast
)
from pyspark.storagelevel import StorageLevel


# ============================================================
# PYTHON CONFIGURATION
# ============================================================

print("=" * 70)
print("PYTHON CONFIGURATION")
print("=" * 70)

print("Python executable:")
print(sys.executable)

print("\nPYSPARK_PYTHON:")
print(os.environ.get("PYSPARK_PYTHON", sys.executable))

print("\nPYSPARK_DRIVER_PYTHON:")
print(os.environ.get("PYSPARK_DRIVER_PYTHON", sys.executable))

print("=" * 70)


# ============================================================
# PROJECT DIRECTORIES
# ============================================================

PROJECT_DIR = Path(__file__).resolve().parent

SP4_DIR = PROJECT_DIR / "sp4_output"
OUTPUT_DIR = PROJECT_DIR / "sp5_output"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# SPARK SESSION
# ============================================================

print("\n" + "=" * 70)
print("SP5 - SPARK PERFORMANCE OPTIMIZATION")
print("=" * 70)

spark = (
    SparkSession.builder
    .appName("SP5_Performance_Optimization")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "14")
    .config("spark.default.parallelism", "14")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\nSpark version:", spark.version)
print("Project directory:", PROJECT_DIR)


# ============================================================
# HELPER FUNCTION - FIND SP4 CSV
# ============================================================

def find_sp4_csv():
    print("\n" + "=" * 70)
    print("SEARCHING FOR SP4 OUTPUT")
    print("=" * 70)

    candidates = []

    if SP4_DIR.exists():
        candidates = list(SP4_DIR.rglob("*.csv"))

    if not candidates:
        raise FileNotFoundError(
            "No SP4 CSV files were found inside:\n"
            f"{SP4_DIR}"
        )

    print("\nSP4 CSV files found:")

    for i, file in enumerate(candidates, start=1):
        print(f"{i}. {file}")

    # Prefer enriched_network_activity.csv
    preferred = [
        f for f in candidates
        if f.name.lower() == "enriched_network_activity.csv"
    ]

    if preferred:
        selected = preferred[0]
    else:
        selected = candidates[0]

    print("\nUsing SP4 file:")
    print(selected)

    return selected


# ============================================================
# FIND INPUT
# ============================================================

sp4_file = find_sp4_csv()


# ============================================================
# LOAD SP4 DATA
# ============================================================

print("\n" + "=" * 70)
print("LOADING SP4 ENRICHED DATA")
print("=" * 70)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(sp4_file))
)

print("\nSP4 schema:")
df.printSchema()

print("\nSP4 columns:")
print(df.columns)

row_count = df.count()

print("\nSP4 row count:")
print(row_count)


# ============================================================
# VALIDATE REQUIRED COLUMNS
# ============================================================

required_columns = [
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_activity",
    "geometry"
]

missing = [
    c for c in required_columns
    if c not in df.columns
]

if missing:
    raise ValueError(
        "Required SP4 columns are missing:\n"
        + "\n".join(f" - {c}" for c in missing)
    )


# ============================================================
# STEP 0 - PREPARE CLEANED DATAFRAME
# ============================================================

print("\n" + "=" * 70)
print("PREPARING CLEANED DATAFRAME")
print("=" * 70)

cleaned_df = (
    df
    .withColumn("date", to_date(col("timestamp")))
)

print("\nCleaned DataFrame columns:")
print(cleaned_df.columns)

print("\nSample:")
cleaned_df.show(5, truncate=False)


# ============================================================
# STEP 1 - EXPLAIN HOTSPOT AGGREGATION
# ============================================================

print("\n" + "=" * 70)
print("STEP 1 - EXPLAIN HOTSPOT AGGREGATION")
print("=" * 70)

print("\nCreating hotspot aggregation...")

hotspot_df = (
    cleaned_df
    .select("grid_id", "total_activity")
    .groupBy("grid_id")
    .agg(
        spark_sum("total_activity").alias("total_activity")
    )
    .orderBy(
        col("total_activity").desc()
    )
    .limit(10)
)

print("\nHotspot result:")
hotspot_df.show()

print("\nPHYSICAL PLAN:")
print("-" * 70)

hotspot_df.explain(mode="formatted")

print("\nObservation:")
print(
    "The plan shows Spark performing aggregation followed by "
    "sorting/limiting to identify the top activity grids."
)


# ============================================================
# STEP 2 - CACHE REUSED CLEANED DATAFRAME
# ============================================================

print("\n" + "=" * 70)
print("STEP 2 - CACHE REUSED CLEANED DATAFRAME")
print("=" * 70)

print("\nTesting repeated action WITHOUT cache...")

start = time.perf_counter()

uncached_count_1 = cleaned_df.count()

uncached_time_1 = time.perf_counter() - start


start = time.perf_counter()

uncached_count_2 = cleaned_df.count()

uncached_time_2 = time.perf_counter() - start


print("\nWithout cache:")
print(
    f"First count: {uncached_count_1} "
    f"Time: {uncached_time_1:.4f} seconds"
)

print(
    f"Second count: {uncached_count_2} "
    f"Time: {uncached_time_2:.4f} seconds"
)


# ------------------------------------------------------------
# CACHE
# ------------------------------------------------------------

print("\nCaching cleaned DataFrame...")

cached_df = cleaned_df.persist(
    StorageLevel.MEMORY_ONLY
)

start = time.perf_counter()

cached_count_materialization = cached_df.count()

cache_materialization_time = (
    time.perf_counter() - start
)


start = time.perf_counter()

cached_count = cached_df.count()

cached_repeated_time = (
    time.perf_counter() - start
)


print("\nWith cache:")
print(
    f"Cache materialization: "
    f"{cache_materialization_time:.4f} seconds"
)

print(
    f"Repeated cached count: {cached_count} "
    f"Time: {cached_repeated_time:.4f} seconds"
)

print("\nCACHE STATUS:")
print(
    "Storage level:",
    cached_df.storageLevel
)

print("\nObservation:")
print(
    "Caching is useful when the same DataFrame is reused for "
    "multiple actions because Spark can avoid recomputing it."
)


# ============================================================
# STEP 3 - REPARTITION BY DATE
# ============================================================

print("\n" + "=" * 70)
print("STEP 3 - REPARTITION BY DATE")
print("=" * 70)

original_partitions = cleaned_df.rdd.getNumPartitions()

print("\nCurrent number of partitions:")
print(original_partitions)


print("\nRepartitioning by date...")

repartitioned_by_date = (
    cleaned_df
    .repartition("date")
)

date_partitions = (
    repartitioned_by_date.rdd.getNumPartitions()
)

print("\nPartitions after repartition by date:")
print(date_partitions)

print("\nPartition observation:")
print(
    "Repartitioning by date causes Spark to redistribute "
    "records using date as the partitioning key."
)


# ------------------------------------------------------------
# CONTROLLED 8 PARTITION VERSION
# ------------------------------------------------------------

print("\nCreating controlled 8-partition version...")

controlled_df = (
    cleaned_df
    .repartition(8)
)

controlled_partitions = (
    controlled_df.rdd.getNumPartitions()
)

print(
    "Controlled partition count:",
    controlled_partitions
)


# ============================================================
# STEP 4 - COLUMN PRUNING
# ============================================================

print("\n" + "=" * 70)
print("STEP 4 - COLUMN PRUNING")
print("=" * 70)

print("\nOriginal columns:")
print(cleaned_df.columns)


# ------------------------------------------------------------
# FULL DATAFRAME AGGREGATION
# ------------------------------------------------------------

print("\nFull DataFrame physical plan:")
print("-" * 70)

full_aggregation = (
    cleaned_df
    .groupBy("grid_id")
    .agg(
        spark_sum("total_activity").alias("total_activity")
    )
)

full_aggregation.explain(mode="formatted")


# ------------------------------------------------------------
# COLUMN PRUNED DATAFRAME
# ------------------------------------------------------------

print("\nColumn-pruned physical plan:")
print("-" * 70)

pruned_df = (
    cleaned_df
    .select(
        "grid_id",
        "total_activity"
    )
)

pruned_aggregation = (
    pruned_df
    .groupBy("grid_id")
    .agg(
        spark_sum("total_activity")
        .alias("total_activity")
    )
)

pruned_aggregation.explain(mode="formatted")

print("\nColumns used after pruning:")
print(pruned_df.columns)

print("\nObservation:")
print(
    "Column pruning reduces the amount of data Spark needs "
    "to carry through the aggregation because unnecessary "
    "columns are removed before the operation."
)


# ============================================================
# STEP 5 - BROADCAST JOIN VS STANDARD JOIN
# ============================================================

print("\n" + "=" * 70)
print("STEP 5 - BROADCAST JOIN VS STANDARD JOIN")
print("=" * 70)

print("\nCreating static grid lookup from SP4...")

grid_lookup = (
    cleaned_df
    .select(
        "grid_id",
        "geometry"
    )
    .filter(
        col("grid_id").isNotNull()
    )
    .dropDuplicates(["grid_id"])
)

grid_lookup_count = grid_lookup.count()

print("\nGrid lookup count:")
print(grid_lookup_count)


# ------------------------------------------------------------
# STANDARD JOIN
# ------------------------------------------------------------

print("\nSTANDARD JOIN PLAN")
print("-" * 70)

standard_join = (
    cleaned_df.alias("activity")
    .join(
        grid_lookup.alias("grid"),
        col("activity.grid_id") == col("grid.grid_id"),
        "left"
    )
    .select(
        col("activity.grid_id"),
        col("activity.timestamp"),
        col("activity.sms_in"),
        col("activity.sms_out"),
        col("activity.call_in"),
        col("activity.call_out"),
        col("activity.internet_activity"),
        col("activity.total_activity"),
        col("activity.geometry"),
        col("activity.date"),
        col("grid.geometry").alias("lookup_geometry")
    )
)

standard_join.explain(mode="formatted")


# ------------------------------------------------------------
# BROADCAST JOIN
# ------------------------------------------------------------

print("\nBROADCAST JOIN PLAN")
print("-" * 70)

broadcast_join = (
    cleaned_df.alias("activity")
    .join(
        broadcast(grid_lookup).alias("grid"),
        col("activity.grid_id") == col("grid.grid_id"),
        "left"
    )
    .select(
        col("activity.grid_id"),
        col("activity.timestamp"),
        col("activity.sms_in"),
        col("activity.sms_out"),
        col("activity.call_in"),
        col("activity.call_out"),
        col("activity.internet_activity"),
        col("activity.total_activity"),
        col("activity.geometry"),
        col("activity.date"),
        col("grid.geometry").alias("lookup_geometry")
    )
)

broadcast_join.explain(mode="formatted")

print("\nBroadcast observation:")
print(
    "The grid lookup contains only one record per grid and "
    "is much smaller than the activity dataset. Therefore "
    "it is a good broadcast candidate."
)

print(
    "\nLook for BroadcastHashJoin in the broadcast plan."
)

print(
    "A broadcast join sends the small lookup table to worker "
    "nodes, which can avoid a large shuffle of the activity data."
)


# ============================================================
# STEP 6 - OVER-PARTITIONING DEMONSTRATION
# ============================================================

print("\n" + "=" * 70)
print("STEP 6 - OVER-PARTITIONING DEMONSTRATION")
print("=" * 70)

print("\nOriginal partitions:")
print(original_partitions)

reasonable_partitions = 8
excessive_partitions = 500

print("\nReasonable partitions:")
print(reasonable_partitions)

print("\nExcessive partitions:")
print(excessive_partitions)


# ------------------------------------------------------------
# REASONABLE PARTITIONS
# ------------------------------------------------------------

reasonable_df = (
    cleaned_df
    .repartition(reasonable_partitions)
)

print("\nTiming reasonable partition count...")

start = time.perf_counter()

reasonable_count = reasonable_df.count()

reasonable_time = (
    time.perf_counter() - start
)

print(
    f"Reasonable partition count time: "
    f"{reasonable_time:.4f} seconds"
)


# ------------------------------------------------------------
# EXCESSIVE PARTITIONS
# ------------------------------------------------------------

excessive_df = (
    cleaned_df
    .repartition(excessive_partitions)
)

print("\nTiming excessive partition count...")

start = time.perf_counter()

excessive_count = excessive_df.count()

excessive_time = (
    time.perf_counter() - start
)

print(
    f"Excessive partition count time: "
    f"{excessive_time:.4f} seconds"
)


print("\nOVER-PARTITIONING OBSERVATION:")

print(
    "A small local dataset does not benefit from hundreds "
    "of partitions. Excessive partitions increase task "
    "scheduling and shuffle overhead."
)


# ============================================================
# STEP 7 - THREE PERFORMANCE OBSERVATIONS
# ============================================================

print("\n" + "=" * 70)
print("STEP 7 - THREE PERFORMANCE OBSERVATIONS")
print("=" * 70)


# ------------------------------------------------------------
# OBSERVATION 1
# ------------------------------------------------------------

print("\nOBSERVATION 1")
print("-" * 70)

print("Observation:")
print("Caching reused DataFrame")

print("\nEvidence:")

print(
    f"  uncached_first_seconds = "
    f"{uncached_time_1:.4f}"
)

print(
    f"  uncached_second_seconds = "
    f"{uncached_time_2:.4f}"
)

print(
    f"  cache_materialization_seconds = "
    f"{cache_materialization_time:.4f}"
)

print(
    f"  cached_repeated_seconds = "
    f"{cached_repeated_time:.4f}"
)

print("\nConclusion:")

print(
    "Caching can reduce repeated computation when the same "
    "DataFrame is reused."
)


# ------------------------------------------------------------
# OBSERVATION 2
# ------------------------------------------------------------

print("\nOBSERVATION 2")
print("-" * 70)

print("Observation:")
print("Broadcast join")

print("\nEvidence:")

print(
    f"  grid_lookup_rows = "
    f"{grid_lookup_count}"
)

print(
    f"  activity_rows = "
    f"{row_count}"
)

print("\nConclusion:")

print(
    "The small grid lookup is suitable for broadcast joining, "
    "which can avoid a large shuffle of the activity dataset."
)


# ------------------------------------------------------------
# OBSERVATION 3
# ------------------------------------------------------------

print("\nOBSERVATION 3")
print("-" * 70)

print("Observation:")
print("Partitioning")

print("\nEvidence:")

print(
    f"  original_partitions = "
    f"{original_partitions}"
)

print(
    f"  date_partitions = "
    f"{date_partitions}"
)

print(
    f"  reasonable_partitions = "
    f"{reasonable_partitions}"
)

print(
    f"  excessive_partitions = "
    f"{excessive_partitions}"
)

print(
    f"  reasonable_time_seconds = "
    f"{reasonable_time:.4f}"
)

print(
    f"  excessive_time_seconds = "
    f"{excessive_time:.4f}"
)

print("\nConclusion:")

print(
    "Too many partitions can increase overhead on a small "
    "local dataset instead of improving performance."
)


# ============================================================
# BUILD PERFORMANCE REPORT
# ============================================================

performance_report = {
    "project": "NP1",
    "phase": "SP5",
    "description": "Spark Performance Optimization",

    "input": {
        "sp4_file": str(sp4_file),
        "row_count": int(row_count),
        "spark_version": spark.version
    },

    "step_1_hotspot": {
        "description": "Hotspot aggregation using groupBy and sum",
        "top_10": [
            {
                "grid_id": int(row["grid_id"]),
                "total_activity": float(row["total_activity"])
            }
            for row in hotspot_df.collect()
        ],
        "observation": (
            "Spark performs aggregation followed by sorting "
            "and limiting to identify top activity grids."
        )
    },

    "step_2_cache": {
        "uncached_first_seconds": round(
            uncached_time_1, 4
        ),
        "uncached_second_seconds": round(
            uncached_time_2, 4
        ),
        "cache_materialization_seconds": round(
            cache_materialization_time, 4
        ),
        "cached_repeated_seconds": round(
            cached_repeated_time, 4
        ),
        "observation": (
            "Caching can reduce repeated computation when "
            "the same DataFrame is reused."
        )
    },

    "step_3_repartition": {
        "original_partitions": int(
            original_partitions
        ),
        "date_partitions": int(
            date_partitions
        ),
        "controlled_partitions": int(
            controlled_partitions
        ),
        "observation": (
            "Repartitioning redistributes records using "
            "the selected partitioning key."
        )
    },

    "step_4_column_pruning": {
        "original_columns": cleaned_df.columns,
        "columns_after_pruning": [
            "grid_id",
            "total_activity"
        ],
        "observation": (
            "Selecting only required columns reduces the "
            "amount of data carried through the aggregation."
        )
    },

    "step_5_broadcast_join": {
        "activity_rows": int(row_count),
        "grid_lookup_rows": int(grid_lookup_count),
        "standard_join": "Standard join plan generated",
        "broadcast_join": "Broadcast join plan generated",
        "expected_operator": "BroadcastHashJoin",
        "observation": (
            "The small static grid lookup is suitable for "
            "broadcast joining."
        )
    },

    "step_6_over_partitioning": {
        "original_partitions": int(
            original_partitions
        ),
        "reasonable_partitions": int(
            reasonable_partitions
        ),
        "excessive_partitions": int(
            excessive_partitions
        ),
        "reasonable_time_seconds": round(
            reasonable_time, 4
        ),
        "excessive_time_seconds": round(
            excessive_time, 4
        ),
        "observation": (
            "Excessive partitioning increases task and "
            "shuffle overhead on small datasets."
        )
    },

    "step_7_three_observations": [
        {
            "observation": "Caching reused DataFrame",
            "evidence": {
                "uncached_first_seconds": round(
                    uncached_time_1, 4
                ),
                "uncached_second_seconds": round(
                    uncached_time_2, 4
                ),
                "cached_repeated_seconds": round(
                    cached_repeated_time, 4
                )
            }
        },
        {
            "observation": "Broadcast join",
            "evidence": {
                "grid_lookup_rows": int(
                    grid_lookup_count
                ),
                "activity_rows": int(
                    row_count
                )
            }
        },
        {
            "observation": "Partitioning",
            "evidence": {
                "original_partitions": int(
                    original_partitions
                ),
                "reasonable_partitions": int(
                    reasonable_partitions
                ),
                "excessive_partitions": int(
                    excessive_partitions
                ),
                "reasonable_time_seconds": round(
                    reasonable_time, 4
                ),
                "excessive_time_seconds": round(
                    excessive_time, 4
                )
            }
        }
    ]
}


# ============================================================
# SAVE PERFORMANCE REPORT
# ============================================================

print("\n" + "=" * 70)
print("SAVING SP5 PERFORMANCE REPORT")
print("=" * 70)

report_path = (
    OUTPUT_DIR /
    "sp5_performance_report.json"
)

with open(
    report_path,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        performance_report,
        f,
        indent=4
    )

print("\nPerformance report saved to:")
print(report_path)


# ============================================================
# SAVE HOTSPOT OUTPUT
# ============================================================
# IMPORTANT:
# Do NOT use:
#
# hotspot_df.write.csv(...)
#
# because Spark's CSV writer invokes Hadoop's Windows
# filesystem layer and requires winutils.exe.
#
# Instead, collect the very small TOP-10 result and use
# Python's standard csv module.
# ============================================================

print("\n" + "=" * 70)
print("SAVING HOTSPOT OUTPUT")
print("=" * 70)

hotspot_output_dir = (
    OUTPUT_DIR /
    "hotspot_top10"
)

hotspot_output_dir.mkdir(
    parents=True,
    exist_ok=True
)

hotspot_csv_path = (
    hotspot_output_dir /
    "hotspot_top10.csv"
)

hotspot_rows = hotspot_df.collect()

with open(
    hotspot_csv_path,
    "w",
    newline="",
    encoding="utf-8"
) as f:

    writer = csv.writer(f)

    writer.writerow([
        "grid_id",
        "total_activity"
    ])

    for row in hotspot_rows:

        writer.writerow([
            row["grid_id"],
            row["total_activity"]
        ])


print("\nHotspot output saved to:")
print(hotspot_csv_path)


# ============================================================
# SAVE PARTITION OBSERVATION
# ============================================================

partition_report_path = (
    OUTPUT_DIR /
    "partition_observation.csv"
)

with open(
    partition_report_path,
    "w",
    newline="",
    encoding="utf-8"
) as f:

    writer = csv.writer(f)

    writer.writerow([
        "configuration",
        "partitions",
        "count_time_seconds"
    ])

    writer.writerow([
        "original",
        original_partitions,
        ""
    ])

    writer.writerow([
        "repartition_by_date",
        date_partitions,
        ""
    ])

    writer.writerow([
        "controlled_8",
        controlled_partitions,
        ""
    ])

    writer.writerow([
        "reasonable",
        reasonable_partitions,
        round(reasonable_time, 4)
    ])

    writer.writerow([
        "excessive",
        excessive_partitions,
        round(excessive_time, 4)
    ])


print("\nPartition observation saved to:")
print(partition_report_path)


# ============================================================
# CLEAN UP CACHE
# ============================================================

cached_df.unpersist()


# ============================================================
# FINAL SUCCESS MESSAGE
# ============================================================

print("\n" + "=" * 70)
print("ALL SP5 OUTPUTS CREATED SUCCESSFULLY")
print("=" * 70)

print("\nOutput directory:")
print(OUTPUT_DIR)

print("\nCreated files:")

print(
    "1.",
    report_path
)

print(
    "2.",
    hotspot_csv_path
)

print(
    "3.",
    partition_report_path
)

print("\nSP5 completed successfully.")

print(
    "\nNOTE: The Hadoop/winutils warning may still appear "
    "at Spark startup on Windows, but it does not affect "
    "this SP5 implementation because output files are "
    "written using Python instead of Spark's Hadoop writer."
)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()
