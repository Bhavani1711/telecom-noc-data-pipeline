import sqlite3
from pathlib import Path

import joblib
import pandas as pd


# ============================================================
# ML6 - BATCH RISK + ANOMALY SCORING
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

DB_PATH = BASE_DIR / "data" / "network_analytics.db"
MODEL_PATH = BASE_DIR / "models" / "ml3_risk_classifier.joblib"

MODEL_VERSION = "ml3-logistic-v1"

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]

CHUNK_SIZE = 50_000


def load_model():
    if not MODEL_PATH.exists():
        raise RuntimeError(
            f"ML3 model artifact not found: {MODEL_PATH}"
        )

    artifact = joblib.load(MODEL_PATH)

    if not isinstance(artifact, dict):
        raise RuntimeError(
            "Invalid ML3 model artifact: expected dictionary."
        )

    if "model" not in artifact:
        raise RuntimeError(
            "Invalid ML3 model artifact: missing model."
        )

    if "feature_columns" not in artifact:
        raise RuntimeError(
            "Invalid ML3 model artifact: missing feature_columns."
        )

    if artifact["feature_columns"] != FEATURE_COLUMNS:
        raise RuntimeError(
            "ML2/ML3 feature schema mismatch."
        )

    return artifact["model"]


def create_staging_table(conn):
    conn.execute(
        "DROP TABLE IF EXISTS network_risk_scores_staging"
    )

    conn.execute(
        """
        CREATE TABLE network_risk_scores_staging (
            grid_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            risk_score REAL NOT NULL,
            risk_level TEXT NOT NULL,
            model_version TEXT NOT NULL
        )
        """
    )

    conn.commit()


def score_features(conn, model):
    query = """
        SELECT
            grid_id,
            feature_timestamp,
            avg_activity,
            activity_growth,
            active_hours,
            peak_ratio,
            variability,
            internet_share
        FROM network_feature_table
        ORDER BY feature_timestamp, grid_id
    """

    total_rows = 0

    chunks = pd.read_sql_query(
        query,
        conn,
        chunksize=CHUNK_SIZE
    )

    for chunk_number, chunk in enumerate(chunks, start=1):

        feature_frame = chunk[FEATURE_COLUMNS]

        probabilities = model.predict_proba(feature_frame)

        classes = list(model.classes_)

        if 1 not in classes:
            raise RuntimeError(
                "ML3 model does not contain class 1."
            )

        class_1_index = classes.index(1)

        risk_scores = probabilities[:, class_1_index]

        output = pd.DataFrame(
            {
                "grid_id": chunk["grid_id"].astype(int),
                "timestamp": chunk["feature_timestamp"].astype(str),
                "risk_score": risk_scores,
            }
        )

        output["risk_level"] = output["risk_score"].apply(
            lambda x:
                "HIGH"
                if x >= 0.70
                else "MEDIUM"
                if x >= 0.40
                else "LOW"
        )

        output["model_version"] = MODEL_VERSION

        output.to_sql(
            "network_risk_scores_staging",
            conn,
            if_exists="append",
            index=False
        )

        total_rows += len(output)

        print(
            f"Scored chunk {chunk_number}: "
            f"{total_rows:,} rows"
        )

    return total_rows


def publish_scores(conn):
    row_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM network_risk_scores_staging
        """
    ).fetchone()[0]

    grid_count = conn.execute(
        """
        SELECT COUNT(DISTINCT grid_id)
        FROM network_risk_scores_staging
        """
    ).fetchone()[0]

    if row_count == 0:
        raise RuntimeError(
            "No risk scores were generated."
        )

    if grid_count == 0:
        raise RuntimeError(
            "No grids were scored."
        )

    # Replace the published table only after successful scoring.
    conn.execute(
        "DROP TABLE IF EXISTS network_risk_scores"
    )

    conn.execute(
        """
        ALTER TABLE network_risk_scores_staging
        RENAME TO network_risk_scores
        """
    )

    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS
        idx_network_risk_grid_timestamp
        ON network_risk_scores(grid_id, timestamp)
        """
    )

    conn.commit()

    return row_count, grid_count


def create_top20_report(conn):
    latest_timestamp = conn.execute(
        """
        SELECT MAX(timestamp)
        FROM network_risk_scores
        """
    ).fetchone()[0]

    query = """
        SELECT
            r.grid_id,
            r.timestamp,
            r.risk_score,
            r.risk_level,
            r.model_version,
            COALESCE(a.anomaly_score, 0.0) AS anomaly_score,
            COALESCE(a.anomaly_direction, 'NO_ML4_RECORD')
                AS anomaly_direction,
            COALESCE(a.ml4_flag, 0) AS ml4_flag,
            COALESCE(a.reason, 'No ML4 anomaly record found.')
                AS anomaly_reason
        FROM network_risk_scores r
        LEFT JOIN network_anomaly_scores a
            ON r.grid_id = a.grid_id
            AND r.timestamp = a.timestamp
        WHERE r.timestamp = ?
        ORDER BY
            r.risk_score DESC,
            anomaly_score DESC
        LIMIT 20
    """

    top20 = pd.read_sql_query(
        query,
        conn,
        params=(latest_timestamp,)
    )

    csv_path = BASE_DIR / "ml6_top20_operational_attention.csv"
    report_path = BASE_DIR / "ML6_Top20_Operational_Attention_Report.md"

    top20.to_csv(csv_path, index=False)

    high_risk_count = int(
        (top20["risk_level"] == "HIGH").sum()
    )

    anomaly_count = int(
        (top20["ml4_flag"] == 1).sum()
    )

    lines = [
        "# ML6 Top-20 Operational Attention Report",
        "",
        "## Objective",
        "",
        "Rank the latest grid-level predictions by trained ML3 "
        "risk probability and provide ML4 anomaly evidence for "
        "operational investigation.",
        "",
        "## Scoring snapshot",
        "",
        f"- Feature/scoring timestamp: `{latest_timestamp}`",
        f"- Model version: `{MODEL_VERSION}`",
        f"- Top-20 high-risk records: `{high_risk_count}`",
        f"- Top-20 records with ML4 anomaly flags: `{anomaly_count}`",
        "",
        "## Top 20",
        "",
        "| Rank | Grid | Risk score | Risk level | ML4 anomaly | Direction | Anomaly score |",
        "|---:|---:|---:|---|---:|---|---:|",
    ]

    for rank, row in enumerate(
        top20.itertuples(index=False),
        start=1
    ):
        lines.append(
            f"| {rank} | {row.grid_id} | "
            f"{row.risk_score:.6f} | {row.risk_level} | "
            f"{row.ml4_flag} | {row.anomaly_direction} | "
            f"{row.anomaly_score:.6f} |"
        )

    lines.extend(
        [
            "",
            "## Operational interpretation",
            "",
            "Higher risk scores indicate higher model-estimated "
            "probability of elevated next-hour activity risk. "
            "ML4 anomaly information provides an independent "
            "historical-deviation signal.",
            "",
            "These outputs are investigation signals and do not "
            "prove congestion, service failure, or a network fault.",
            "",
            "## Outputs",
            "",
            f"- `{csv_path.name}`",
            f"- `{report_path.name}`",
            f"- `network_risk_scores` SQLite table",
            "",
        ]
    )

    report_path.write_text(
        "\n".join(lines),
        encoding="utf-8"
    )

    print(f"Top-20 CSV: {csv_path}")
    print(f"Top-20 report: {report_path}")


def validate_output(conn):
    row_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM network_risk_scores
        """
    ).fetchone()[0]

    grid_count = conn.execute(
        """
        SELECT COUNT(DISTINCT grid_id)
        FROM network_risk_scores
        """
    ).fetchone()[0]

    min_timestamp, max_timestamp = conn.execute(
        """
        SELECT MIN(timestamp), MAX(timestamp)
        FROM network_risk_scores
        """
    ).fetchone()

    invalid_levels = conn.execute(
        """
        SELECT COUNT(*)
        FROM network_risk_scores
        WHERE risk_level NOT IN ('LOW', 'MEDIUM', 'HIGH')
        """
    ).fetchone()[0]

    if invalid_levels != 0:
        raise RuntimeError(
            f"Invalid risk levels found: {invalid_levels}"
        )

    print("\n=== ML6 VALIDATION ===")
    print(f"Rows: {row_count:,}")
    print(f"Distinct grids: {grid_count:,}")
    print(f"Timestamp range: {min_timestamp} -> {max_timestamp}")
    print("Risk levels: PASS")
    print("Schema validation: PASS")


def main():
    if not DB_PATH.exists():
        raise RuntimeError(
            f"Database not found: {DB_PATH}"
        )

    print("Loading ML3 model...")
    model = load_model()

    print(f"Model version: {MODEL_VERSION}")
    print(f"Feature columns: {FEATURE_COLUMNS}")

    conn = sqlite3.connect(DB_PATH)

    try:
        print("\nCreating staging table...")
        create_staging_table(conn)

        print("\nRunning batch risk scoring...")
        scored_rows = score_features(conn, model)

        print(
            f"\nBatch scoring complete: "
            f"{scored_rows:,} rows"
        )

        print("\nPublishing network_risk_scores...")
        row_count, grid_count = publish_scores(conn)

        print(
            f"Published {row_count:,} rows "
            f"for {grid_count:,} grids."
        )

        validate_output(conn)

        print("\nGenerating Top-20 operational report...")
        create_top20_report(conn)

        print("\nML6 BATCH SCORING COMPLETE.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
