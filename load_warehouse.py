cat spark/telecom_pipeline.pyimport os
import sqlite3
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))

PARQUET_DIR = os.path.join(
    BASE, "sp7_output", "hourly_grid_summary"
)

DB_PATH = os.path.join(
    BASE, "data", "network_analytics.db"
)


def main():
    print("Loading warehouse...")

    files = []

    for root, dirs, filenames in os.walk(PARQUET_DIR):
        for filename in filenames:
            if filename.endswith(".parquet"):
                files.append(os.path.join(root, filename))

    if not files:
        raise RuntimeError(
            f"No parquet files found in {PARQUET_DIR}"
        )

    print(f"Found {len(files)} parquet files.")

    frames = []

    for file in files:
        print(f"Reading: {file}")
        temp = pd.read_parquet(file)

        # Get date from partition folder: date=YYYY-MM-DD
        date_value = None

        for part in file.split(os.sep):
            if part.startswith("date="):
                date_value = part.replace("date=", "")
                break

        if date_value is None:
            raise RuntimeError(
                f"Could not determine date from {file}"
            )

        temp["timestamp"] = (
            pd.to_datetime(date_value)
            + pd.to_timedelta(temp["hour"], unit="h")
        )

        frames.append(temp)

    df = pd.concat(frames, ignore_index=True)

    print(f"Rows read: {len(df)}")

    columns = [
        "grid_id",
        "hour",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
        "total_activity",
        "record_count",
        "avg_activity_per_record",
        "timestamp"
    ]

    missing = [c for c in columns if c not in df.columns]

    if missing:
        raise RuntimeError(f"Missing columns: {missing}")

    df = df[columns]

    before = len(df)

    df = df.drop_duplicates(
        subset=["grid_id", "timestamp"]
    )

    print(f"Duplicates removed: {before - len(df)}")

    os.makedirs(
        os.path.dirname(DB_PATH),
        exist_ok=True
    )

    print("Writing to data/network_analytics.db...")

    conn = sqlite3.connect(DB_PATH)

    try:
        df.to_sql(
            "hourly_grid_summary",
            conn,
            if_exists="replace",
            index=False
        )
        conn.commit()
    finally:
        conn.close()

    conn = sqlite3.connect(DB_PATH)

    count = conn.execute(
        "SELECT COUNT(*) FROM hourly_grid_summary"
    ).fetchone()[0]

    minimum = conn.execute(
        "SELECT MIN(timestamp) FROM hourly_grid_summary"
    ).fetchone()[0]

    maximum = conn.execute(
        "SELECT MAX(timestamp) FROM hourly_grid_summary"
    ).fetchone()[0]

    conn.close()

    print()
    print("================================")
    print("WAREHOUSE LOAD COMPLETE")
    print("================================")
    print(f"Rows: {count}")
    print(f"MIN timestamp: {minimum}")
    print(f"MAX timestamp: {maximum}")
    print("================================")


if __name__ == "__main__":
    main()
