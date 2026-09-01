import os
import glob
from pyspark.sql import SparkSession


# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_DIR = r"C:\Users\bhavani.as\Desktop\NP1"


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("NP1 Telecom Data Ingestion")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("SPARK SESSION CREATED")
print("=" * 60)
print("Spark version:", spark.version)


# ============================================================
# SEARCH FOR CSV FILES
# ============================================================

print()
print("=" * 60)
print("SEARCHING FOR INPUT FILES")
print("=" * 60)

print("Project directory:")
print(PROJECT_DIR)

# Search recursively inside NP1
pattern = os.path.join(
    PROJECT_DIR,
    "**",
    "sms-call-internet-mi-*.csv"
)

print()
print("Search pattern:")
print(pattern)

files = glob.glob(pattern, recursive=True)


# ============================================================
# CHECK WHETHER FILES WERE FOUND
# ============================================================

if not files:
    print()
    print("ERROR: No telecom CSV files were found.")
    print()
    print("The script searched EVERY subfolder inside:")
    print(PROJECT_DIR)
    print()
    print("Expected filename format:")
    print("sms-call-internet-mi-YYYY-MM-DD.csv")
    print()
    print("Please check that your dataset files have that filename pattern.")
    spark.stop()
    raise SystemExit(1)


# ============================================================
# DISPLAY FOUND FILES
# ============================================================

print()
print("FOUND", len(files), "CSV FILE(S):")
print()

for i, file in enumerate(files, start=1):
    print(f"{i}. {file}")


# ============================================================
# READ THE CSV FILES
# ============================================================

print()
print("=" * 60)
print("READING DATA")
print("=" * 60)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(files)
)


# ============================================================
# BASIC INSPECTION
# ============================================================

print()
print("=" * 60)
print("DATASET INFORMATION")
print("=" * 60)

print("Number of rows:", df.count())
print("Number of columns:", len(df.columns))

print()
print("Columns:")
print(df.columns)

print()
print("Schema:")
df.printSchema()

print()
print("Sample records:")
df.show(10, truncate=False)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()

print()
print("=" * 60)
print("SPARK JOB COMPLETED SUCCESSFULLY")
print("=" * 60)