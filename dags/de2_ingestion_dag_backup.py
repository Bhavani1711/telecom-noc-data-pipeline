from datetime import datetime
from pathlib import Path
import shutil
import csv
import subprocess
import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator


# ============================================================
# PROJECT PATHS
# ============================================================

BASE_DIR = Path("/mnt/c/Users/bhavani.as/Desktop/NP1")

LANDING_DIR = BASE_DIR / "data" / "landing" / "raw_csv_files"
RAW_DIR = BASE_DIR / "data" / "raw"
REJECTED_DIR = BASE_DIR / "data" / "rejected"
LOG_DIR = BASE_DIR / "logs"

PROCESSED_DIR = BASE_DIR / "data" / "processed"
ANALYTICS_DIR = BASE_DIR / "data" / "analytics"

REFERENCE_FILE = BASE_DIR / "data" / "reference" / "milano-grid.geojson"
SPARK_SCRIPT = BASE_DIR / "spark" / "telecom_pipeline.py"

DETECTED_FILE = LOG_DIR / "detected_files.txt"
VALIDATION_FILE = LOG_DIR / "validation_results.csv"
METADATA_FILE = LOG_DIR / "ingestion_metadata.csv"


# ============================================================
# EXPECTED COLUMNS
# ============================================================

REQUIRED_COLUMNS = {
    "datetime",
    "CellID",
    "countrycode",
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
}

ACTIVITY_COLUMNS = {
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
}


# ============================================================
# DE2 - DETECT FILES
# ============================================================

def detect_files():

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(LANDING_DIR.glob("sms-call-internet-mi-*.csv"))

    if not files:
        raise FileNotFoundError(
            f"No input CSV files found in {LANDING_DIR}"
        )

    with open(DETECTED_FILE, "w") as f:
        for file in files:
            f.write(str(file) + "\n")

    print(f"Detected {len(files)} CSV file(s).")

    for file in files:
        print(file)


# ============================================================
# DE2 - VALIDATE FILES
# ============================================================

def validate_files():

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(LANDING_DIR.glob("sms-call-internet-mi-*.csv"))

    if not files:
        raise FileNotFoundError("No files available for validation.")

    results = []

    for file in files:

        valid = True
        reason = ""

        try:

            with open(file, "r", newline="", encoding="utf-8") as f:

                reader = csv.DictReader(f)

                columns = set(reader.fieldnames or [])

                missing = REQUIRED_COLUMNS - columns

                if missing:
                    valid = False
                    reason = f"Missing columns: {sorted(missing)}"

                else:

                    for row_number, row in enumerate(reader, start=2):

                        for column in ACTIVITY_COLUMNS:

                            value = row.get(column)

                            try:
                                number = float(value)

                                if number < 0:
                                    valid = False
                                    reason = (
                                        f"Negative value in {column} "
                                        f"at row {row_number}"
                                    )
                                    break

                            except (TypeError, ValueError):

                                valid = False
                                reason = (
                                    f"Invalid numeric value in {column} "
                                    f"at row {row_number}"
                                )
                                break

                        if not valid:
                            break

        except Exception as e:

            valid = False
            reason = str(e)

        results.append(
            {
                "file": file.name,
                "valid": valid,
                "reason": reason,
            }
        )

    with open(
        VALIDATION_FILE,
        "w",
        newline="",
        encoding="utf-8",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=["file", "valid", "reason"],
        )

        writer.writeheader()
        writer.writerows(results)

    print("Validation completed.")

    for result in results:
        print(result)


# ============================================================
# DE2 - ROUTE FILES
# ============================================================

def route_files():

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)

    if not VALIDATION_FILE.exists():
        raise FileNotFoundError(
            f"Validation file not found: {VALIDATION_FILE}"
        )

    with open(
        VALIDATION_FILE,
        "r",
        newline="",
        encoding="utf-8",
    ) as f:

        reader = csv.DictReader(f)

        for row in reader:

            source = LANDING_DIR / row["file"]

            if not source.exists():
                continue

            if row["valid"].lower() == "true":

                destination = RAW_DIR / source.name

            else:

                destination = REJECTED_DIR / source.name

            shutil.move(str(source), str(destination))

            print(
                f"Moved {source.name} -> {destination.parent}"
            )


# ============================================================
# DE2 - LOG INGESTION
# ============================================================

def log_files():

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    rows = []

    for file in sorted(RAW_DIR.glob("*.csv")):

        rows.append(
            {
                "file": file.name,
                "status": "RAW",
                "path": str(file),
            }
        )

    for file in sorted(REJECTED_DIR.glob("*.csv")):

        rows.append(
            {
                "file": file.name,
                "status": "REJECTED",
                "path": str(file),
            }
        )

    with open(
        METADATA_FILE,
        "w",
        newline="",
        encoding="utf-8",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=["file", "status", "path"],
        )

        writer.writeheader()
        writer.writerows(rows)

    print("Ingestion metadata logged.")


# ============================================================
# DE3 - RUN SPARK PROCESSING
# ============================================================

def run_spark_processing():

    print("=" * 60)
    print("DE3 SPARK PROCESSING STARTED")
    print("=" * 60)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

    if not SPARK_SCRIPT.exists():

        raise FileNotFoundError(
            f"Spark script not found: {SPARK_SCRIPT}"
        )

    if not REFERENCE_FILE.exists():

        raise FileNotFoundError(
            f"GeoJSON reference not found: {REFERENCE_FILE}"
        )

    raw_files = sorted(RAW_DIR.glob("*.csv"))

    if not raw_files:

        raise FileNotFoundError(
            f"No raw CSV files found in {RAW_DIR}"
        )

    print(f"Raw files available: {len(raw_files)}")
    print(f"Spark script: {SPARK_SCRIPT}")
    print(f"Reference: {REFERENCE_FILE}")

    python_executable = sys.executable

    command = [
        python_executable,
        str(SPARK_SCRIPT),
        "--input",
        str(RAW_DIR),
        "--output",
        str(PROCESSED_DIR),
        "--reference",
        str(REFERENCE_FILE),
    ]

    print("Running Spark command:")

    print(" ".join(command))

    result = subprocess.run(
        command,
        cwd=str(BASE_DIR),
        env=os.environ.copy(),
        check=False,
    )

    print(f"Spark return code: {result.returncode}")

    if result.returncode != 0:

        print("DE3 SPARK PROCESSING FAILED")

        raise RuntimeError(
            f"Spark pipeline failed with return code "
            f"{result.returncode}"
        )

    print("=" * 60)
    print("DE3 SPARK PROCESSING SUCCESS")
    print("=" * 60)


# ============================================================
# AIRFLOW DAG
# ============================================================

with DAG(
    dag_id="de2_landing_to_raw",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["DE2", "DE3", "ingestion"],
) as dag:

    detect = PythonOperator(
        task_id="detect_files",
        python_callable=detect_files,
    )

    validate = PythonOperator(
        task_id="validate_files",
        python_callable=validate_files,
    )

    route = PythonOperator(
        task_id="route_files",
        python_callable=route_files,
    )

    log = PythonOperator(
        task_id="log_ingestion",
        python_callable=log_files,
    )

    spark_processing = PythonOperator(
        task_id="spark_processing",
        python_callable=run_spark_processing,
    )

    detect >> validate >> route >> log >> spark_processing
