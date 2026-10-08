from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule

from datetime import datetime
from pathlib import Path
import json
import shutil
import subprocess

import pandas as pd


# ============================================================
# PATHS
# ============================================================

PROJECT_ROOT = Path("/mnt/c/Users/bhavani.as/Desktop/NP1")

LANDING_DIR = PROJECT_ROOT / "data" / "landing"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
REJECTED_DIR = PROJECT_ROOT / "data" / "rejected"
ANALYTICS_DIR = PROJECT_ROOT / "data" / "analytics"

STATUS_FILE = ANALYTICS_DIR / "pipeline_status.json"


# ============================================================
# EXPECTED DATASET SCHEMA
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

ACTIVITY_COLUMNS = [
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
]


# ============================================================
# DE7 TASK 1 — INGEST
# ============================================================

def ingest(**context):

    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    files = list(LANDING_DIR.glob("*.csv"))

    print(f"Found {len(files)} CSV file(s) in landing.")

    if not files:
        raise FileNotFoundError(
            "No CSV files found in data/landing."
        )

    for source in files:

        destination = RAW_DIR / source.name

        # Duplicate ingestion protection
        if destination.exists():

            print(f"DUPLICATE detected: {source.name}")
            print("Skipping duplicate ingestion.")

            continue

        shutil.move(str(source), str(destination))

        print(f"Ingested: {source.name}")
        print(f"Raw location: {destination}")

    return "success"


# ============================================================
# DE7 TASK 2 — VALIDATE
# ============================================================

def validate(**context):

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)

    files = list(RAW_DIR.glob("*.csv"))

    if not files:
        raise FileNotFoundError(
            "No CSV files found in data/raw."
        )

    for file_path in files:

        print(f"Validating: {file_path.name}")

        try:

            # Read only the header first
            header = pd.read_csv(
                file_path,
                nrows=0
            )

            actual_columns = set(header.columns)

            # -----------------------------
            # Missing columns
            # -----------------------------

            missing = REQUIRED_COLUMNS - actual_columns

            if missing:

                raise ValueError(
                    f"Missing columns: {sorted(missing)}"
                )

            # -----------------------------
            # Unexpected columns
            # -----------------------------

            unexpected = actual_columns - REQUIRED_COLUMNS

            if unexpected:

                raise ValueError(
                    f"Unexpected columns: {sorted(unexpected)}"
                )

            # -----------------------------
            # Read data
            # -----------------------------

            df = pd.read_csv(file_path)

            # -----------------------------
            # Validate datetime
            # -----------------------------

            parsed_datetime = pd.to_datetime(
                df["datetime"],
                errors="coerce"
            )

            bad_datetime = parsed_datetime.isna().sum()

            if bad_datetime > 0:

                raise ValueError(
                    f"Malformed datetime values: {bad_datetime}"
                )

            # -----------------------------
            # Validate negative activity
            # -----------------------------

            for column in ACTIVITY_COLUMNS:

                negative_count = (
                    pd.to_numeric(
                        df[column],
                        errors="coerce"
                    )
                    .lt(0)
                    .sum()
                )

                if negative_count > 0:

                    raise ValueError(
                        f"Negative values found in "
                        f"{column}: {negative_count}"
                    )

            print(
                f"VALIDATION PASSED: {file_path.name}"
            )

        except Exception as exc:

            rejected_path = REJECTED_DIR / file_path.name

            if rejected_path.exists():
                rejected_path.unlink()

            shutil.move(
                str(file_path),
                str(rejected_path)
            )

            print("FILE QUARANTINED")
            print(f"File: {file_path.name}")
            print(f"Reason: {exc}")
            print(f"Rejected location: {rejected_path}")

            raise

    return "success"


# ============================================================
# DE7 TASK 3 — SPARK PROCESS
# ============================================================

def spark_process(**context):

    command = [
        "python",
        "spark/telecom_pipeline.py",
        "--input",
        "data/raw",
        "--output",
        "data/processed",
        "--reference",
        "data/reference/milano-grid.geojson",
    ]

    print("Running Spark processing:")
    print(" ".join(command))

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
    )

    if result.returncode != 0:

        raise RuntimeError(
            f"Spark processing failed with exit code "
            f"{result.returncode}"
        )

    print("Spark processing completed successfully.")

    return "success"


# ============================================================
# DE7 TASK 4 — LOAD WAREHOUSE
# ============================================================

def load_warehouse(**context):

    command = [
        "python",
        "de6_warehouse.py",
    ]

    print("Running warehouse load:")
    print(" ".join(command))

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
    )

    if result.returncode != 0:

        raise RuntimeError(
            f"Warehouse load failed with exit code "
            f"{result.returncode}"
        )

    print("Warehouse load completed successfully.")

    return "success"

# ============================================================
# ML6 TASK — FEATURE GENERATION
# ============================================================

def feature_generation(**context):

    command = [
        "python",
        "ml2_feature_engineering.py",
    ]

    print("Running ML2 feature generation:")
    print(" ".join(command))

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
    )

    if result.returncode != 0:

        raise RuntimeError(
            f"ML2 feature generation failed with exit code "
            f"{result.returncode}"
        )

    print("ML2 feature generation completed successfully.")

    return "success"


# ============================================================
# ML6 TASK — BATCH RISK SCORING
# ============================================================

def ml6_batch_scoring(**context):

    command = [
        "python",
        "ml6_batch_scoring.py",
    ]

    print("Running ML6 batch scoring:")
    print(" ".join(command))

    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        text=True,
    )

    if result.returncode != 0:

        raise RuntimeError(
            f"ML6 batch scoring failed with exit code "
            f"{result.returncode}"
        )

    print("ML6 batch scoring completed successfully.")

    return "success"


# ============================================================
# DE7 TASK 5 — QUALITY CHECK / STATUS
# ============================================================

def write_status(**context):

    ANALYTICS_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    ti = context["ti"]
    dag_run = context.get("dag_run")

    status = {

        "run_id": (
            dag_run.run_id
            if dag_run
            else "unknown"
        ),

        "timestamp": datetime.now().isoformat(),

        "status": "SUCCESS",

        "ingest": "not_run",
        "validate": "not_run",
        "spark_process": "not_run",
        "load_warehouse": "not_run",
        "feature_generation": "not_run",
        "ml6_batch_scoring": "not_run",

        "quality_check": "success",
    }

    task_ids = [
    "ingest",
    "validate",
    "spark_process",
    "load_warehouse",
    "feature_generation",
    "ml6_batch_scoring",

    ]

    for task_id in task_ids:

        result = ti.xcom_pull(
            task_ids=task_id,
            key="return_value"
        )

        if result == "success":

            status[task_id] = "success"

        else:

            status[task_id] = "failed"
            status["status"] = "FAILED"

    with open(
        STATUS_FILE,
        "w"
    ) as f:

        json.dump(
            status,
            f,
            indent=2
        )

    print("===================================")
    print("DE7 PIPELINE STATUS")
    print("===================================")
    print(
        json.dumps(
            status,
            indent=2
        )
    )
    print(
        f"Status file: {STATUS_FILE}"
    )
    print("===================================")

    if status["status"] == "FAILED":

        raise RuntimeError(
            "Pipeline failed. See pipeline_status.json"
        )

    return "success"


# ============================================================
# DE7 TASK 6 — NOTIFICATION
# ============================================================

def notify(**context):

    print("===================================")
    print("DE7 PIPELINE NOTIFICATION")
    print("===================================")

    print("Pipeline execution finished.")
    print(
        f"Status file: {STATUS_FILE}"
    )

    if not STATUS_FILE.exists():

        raise FileNotFoundError(
            "pipeline_status.json was not created."
        )

    with open(
        STATUS_FILE,
        "r"
    ) as f:

        status = json.load(f)

    pipeline_status = status.get(
        "status",
        "UNKNOWN"
    )

    print(
        f"Final pipeline status: "
        f"{pipeline_status}"
    )

    print("===================================")

    if pipeline_status == "FAILED":

        raise RuntimeError(
            "DE7 pipeline failed. "
            "See pipeline_status.json"
        )

    print("DE7 SUCCESS NOTIFICATION")

    return "success"


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="de7_network_pipeline",

    start_date=datetime(
        2026,
        1,
        1
    ),

    schedule=None,

    catchup=False,

    tags=[
        "NP1",
        "DE7",
        "network",
        "pipeline",
    ],

) as dag:

    ingest_task = PythonOperator(
        task_id="ingest",
        python_callable=ingest,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    spark_task = PythonOperator(
        task_id="spark_process",
        python_callable=spark_process,
    )

    warehouse_task = PythonOperator(
        task_id="load_warehouse",
        python_callable=load_warehouse,
    )
    feature_task = PythonOperator(
        task_id="feature_generation",
        python_callable=feature_generation,
    )

    scoring_task = PythonOperator(
        task_id="ml6_batch_scoring",
        python_callable=ml6_batch_scoring,
    )

    quality_task = PythonOperator(
        task_id="quality_check",
        python_callable=write_status,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    notify_task = PythonOperator(
        task_id="notify",
        python_callable=notify,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    ingest_task >> validate_task
    validate_task >> spark_task
    spark_task >> warehouse_task
    warehouse_task >> feature_task
    feature_task >> scoring_task
    scoring_task >> quality_task
    quality_task >> notify_task
