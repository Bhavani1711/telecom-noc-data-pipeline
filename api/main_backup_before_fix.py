from pathlib import Path
import sqlite3
import csv
import json
import joblib
import pandas as pd

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


# ============================================================
# PATHS
# ============================================================

BASE_DIR = Path(__file__).resolve().parent.parent

DB_PATH = BASE_DIR / "data" / "network_analytics.db"
ALERTS_PATH = BASE_DIR / "network_alerts.csv"
HOTSPOTS_PATH = BASE_DIR / "sp5_output" / "hotspot_top10" / "hotspot_top10.csv"

# ============================================================
# ML5 - MODEL SERVING
# ============================================================

MODEL_PATH = BASE_DIR / "models" / "ml3_risk_classifier.joblib"
MODEL_VERSION = "ml3-logistic-v1"

if not MODEL_PATH.exists():
    raise RuntimeError(
        f"ML3 model artifact not found: {MODEL_PATH}"
    )

ML_ARTIFACT = joblib.load(MODEL_PATH)

if not isinstance(ML_ARTIFACT, dict):
    raise RuntimeError(
        "Invalid ML3 model artifact: expected a dictionary package."
    )

if "model" not in ML_ARTIFACT:
    raise RuntimeError(
        "Invalid ML3 model artifact: missing 'model'."
    )

if "feature_columns" not in ML_ARTIFACT:
    raise RuntimeError(
        "Invalid ML3 model artifact: missing 'feature_columns'."
    )

ML_MODEL = ML_ARTIFACT["model"]
ML_FEATURE_COLUMNS = ML_ARTIFACT["feature_columns"]

if not ML_FEATURE_COLUMNS:
    raise RuntimeError(
        "Invalid ML3 model artifact: feature_columns is empty."
    )


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="Network Intelligence API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# AP1 - NETWORK SUMMARY
# ============================================================

class NetworkSummary(BaseModel):
    total_activity: float
    active_grids: int
    peak_hour: str
    top_grid: int
    as_of: str


def get_connection():
    if not DB_PATH.exists():
        raise RuntimeError(f"Warehouse not found: {DB_PATH}")

    return sqlite3.connect(DB_PATH)


@app.get("/network/summary", response_model=NetworkSummary)
def network_summary(
    as_of: str | None = Query(
        default=None,
        description="Optional reporting timestamp"
    )
):
    conn = None

    try:
        conn = get_connection()

        if as_of is None:
            row = conn.execute(
                "SELECT MAX(timestamp) FROM hourly_grid_summary"
            ).fetchone()

            effective_as_of = row[0]

            if effective_as_of is None:
                raise RuntimeError("Analytics table contains no data")
        else:
            effective_as_of = as_of

        row = conn.execute(
            """
            SELECT
                COALESCE(SUM(total_activity), 0),
                COUNT(DISTINCT grid_id)
            FROM hourly_grid_summary
            WHERE timestamp = ?
            """,
            (effective_as_of,)
        ).fetchone()

        total_activity = row[0]
        active_grids = row[1]

        row = conn.execute(
            """
            SELECT
                timestamp,
                activity
            FROM hourly_activity_totals
            WHERE timestamp <= ?
            ORDER BY activity DESC, timestamp ASC
            LIMIT 1
            """,
            (effective_as_of,)
        ).fetchone()

        if row is None:
            raise RuntimeError("No analytics data available")

        peak_hour = row[0]

        row = conn.execute(
            """
            SELECT
                grid_id,
                SUM(total_activity) AS activity
            FROM hourly_grid_summary
            WHERE timestamp = ?
            GROUP BY grid_id
            ORDER BY activity DESC, grid_id ASC
            LIMIT 1
            """,
            (effective_as_of,)
        ).fetchone()

        if row is None:
            raise RuntimeError("No grid data available")

        top_grid = row[0]

        return NetworkSummary(
            total_activity=float(total_activity),
            active_grids=int(active_grids),
            peak_hour=str(peak_hour),
            top_grid=int(top_grid),
            as_of=str(effective_as_of)
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Analytics data source unavailable: {exc}"
        )

    finally:
        if conn is not None:
            conn.close()


# ============================================================
# AP2 - GRID ACTIVITY DRILL-DOWN
# ============================================================

@app.get("/network/grid/{grid_id}")
def grid_activity(
    grid_id: int,
    date: str | None = Query(
        default=None,
        description="Optional date filter YYYY-MM-DD"
    ),
    hour: int | None = Query(
        default=None,
        ge=0,
        le=23,
        description="Optional hour filter 0-23"
    ),
    as_of: str | None = Query(
        default=None,
        description="Optional ending timestamp"
    )
):
    # Grid IDs outside 1-10000 are unknown
    if grid_id < 1 or grid_id > 10000:
        raise HTTPException(
            status_code=404,
            detail=f"Grid {grid_id} not found"
        )

    conn = None

    try:
        conn = get_connection()

        # Check whether grid exists
        exists = conn.execute(
            """
            SELECT 1
            FROM hourly_grid_summary
            WHERE grid_id = ?
            LIMIT 1
            """,
            (grid_id,)
        ).fetchone()

        if exists is None:
            raise HTTPException(
                status_code=404,
                detail=f"Grid {grid_id} not found"
            )

        # Determine ending timestamp
        if as_of is None:
            row = conn.execute(
                """
                SELECT MAX(timestamp)
                FROM hourly_grid_summary
                WHERE grid_id = ?
                """,
                (grid_id,)
            ).fetchone()

            effective_as_of = row[0]
        else:
            effective_as_of = as_of

        # Build query
        query = """
            SELECT
                grid_id,
                timestamp,
                hour,
                sms_in,
                sms_out,
                call_in,
                call_out,
                internet_activity,
                total_activity,
                record_count,
                avg_activity_per_record
            FROM hourly_grid_summary
            WHERE grid_id = ?
        """

        params = [grid_id]

        # Date filter
        if date is not None:
            query += " AND DATE(timestamp) = ?"
            params.append(date)

        # Hour filter
        if hour is not None:
            query += " AND hour = ?"
            params.append(hour)

        # Default trailing 24-hour window
        if date is None and hour is None:
            query += """
                AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT 24
            """
            params.append(effective_as_of)
        else:
            query += """
                ORDER BY timestamp DESC
            """

        rows = conn.execute(query, params).fetchall()

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No activity found for grid {grid_id}"
            )

        # Return chronological order
        rows = list(reversed(rows))

        activity = []

        for row in rows:
            activity.append({
                "grid_id": int(row[0]),
                "timestamp": str(row[1]),
                "hour": int(row[2]),
                "sms_in": float(row[3] or 0),
                "sms_out": float(row[4] or 0),
                "call_in": float(row[5] or 0),
                "call_out": float(row[6] or 0),
                "internet_activity": float(row[7] or 0),
                "total_activity": float(row[8] or 0),
                "record_count": int(row[9] or 0),
                "avg_activity_per_record": float(row[10] or 0)
            })

        return {
            "grid_id": grid_id,
            "as_of": str(effective_as_of),
            "count": len(activity),
            "activity": activity
        }

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Grid activity unavailable: {exc}"
        )

    finally:
        if conn is not None:
            conn.close()

def get_ml6_risk(grid_id: int, timestamp: str | None = None):
    conn = None

    try:
        conn = get_connection()

        if timestamp is None:
            row = conn.execute(
                """
                SELECT
                    risk_score,
                    risk_level,
                    model_version,
                    timestamp
                FROM network_risk_scores
                WHERE grid_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (grid_id,)
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT
                    risk_score,
                    risk_level,
                    model_version,
                    timestamp
                FROM network_risk_scores
                WHERE grid_id = ?
                  AND timestamp = ?
                LIMIT 1
                """,
                (grid_id, timestamp)
            ).fetchone()

        if row is None:
            return {
                "risk_score": None,
                "risk_level": None,
                "model_version": None,
                "timestamp": timestamp
            }

        return {
            "risk_score": float(row[0]),
            "risk_level": row[1],
            "model_version": row[2],
            "timestamp": row[3]
        }

    except Exception as exc:
        raise RuntimeError(
            f"ML6 risk lookup failed: {exc}"
        )

# ============================================================
# AP3 - HOTSPOTS
# ============================================================

@app.get("/network/hotspots")
def hotspots(
    limit: int = Query(default=10, ge=1, le=100),
    as_of: str | None = Query(
        default=None,
        description="Reserved for future timestamp-specific hotspot data"
    )
):
    if not HOTSPOTS_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail="Hotspot output file not found"
        )

    try:
        rows = []

        with open(HOTSPOTS_PATH, newline="") as f:
            reader = csv.DictReader(f)

            for row in reader:
                grid_id = int(row["grid_id"])
                risk = get_ml6_risk(grid_id)

                rows.append({
                    "grid_id": grid_id,
                    "total_activity": float(row["total_activity"]),
                    "risk_score": risk["risk_score"],
                    "risk_level": risk["risk_level"],
                    "model_version": risk["model_version"]
                })

        rows.sort(
            key=lambda x: x["total_activity"],
            reverse=True
        )

        rows = rows[:limit]

        return {
            "as_of": as_of,
            "count": len(rows),
            "hotspots": rows
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Hotspot data unavailable: {exc}"
        )


# AP3 - ALERTS
# ============================================================

@app.get("/network/alerts")
def alerts(
    limit: int = Query(
        default=100,
        ge=1,
        le=1000
    ),
    severity: str | None = Query(
        default=None,
        description="Optional severity/alert type filter"
    ),
    as_of: str | None = Query(
        default=None,
        description="Optional timestamp filter"
    )
):
    if not ALERTS_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail="Alert output file not found"
        )

    try:
        with open(ALERTS_PATH, newline="") as f:
            reader = csv.DictReader(f)

            rows = []

            for row in reader:

                # Filter by severity/alert type
                if severity is not None:
                    if row["alert_type"].lower() != severity.lower():
                        continue

                # Filter by timestamp
                if as_of is not None:
                    if row["timestamp"] != as_of:
                        continue

                rows.append({
                    "grid_id": int(row["grid_id"]),
                    "timestamp": row["timestamp"],
                    "status": "ALERT",
                    "severity": row["alert_type"],
                    "alert_type": row["alert_type"],
                    "current_activity": float(row["current_activity"]),
                    "baseline_activity": float(row["baseline_activity"]),
                    "reason": row["reason"],

                    # Future ML fields
                    "ml_risk_score": None,
                    "ml_risk_level": None
                })

        rows = rows[:limit]

        return {
            "as_of": as_of,
            "count": len(rows),
            "alerts": rows
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Alert data unavailable: {exc}"
        )
# ============================================================
# API4 - GRID FEATURES
# ============================================================

class GridFeatures(BaseModel):
    grid_id: int
    avg_activity: float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share: float
    feature_timestamp: str
    data_quality_status: str
    feature_freshness: str


@app.get("/network/grid/{grid_id}/features", response_model=GridFeatures)
def grid_features(grid_id: int):

    if grid_id < 1 or grid_id > 10000:
        raise HTTPException(
            status_code=404,
            detail=f"Grid {grid_id} not found"
        )

    conn = None

    try:
        conn = get_connection()

        row = conn.execute(
            """
            SELECT
                grid_id,
                avg_activity,
                activity_growth,
                active_hours,
                peak_ratio,
                variability,
                internet_share,
                feature_timestamp
            FROM ml2_features
            WHERE grid_id = ?
            """,
            (grid_id,)
        ).fetchone()

        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"Features not found for grid {grid_id}"
            )

        return GridFeatures(
            grid_id=int(row[0]),
            avg_activity=float(row[1] or 0),
            activity_growth=float(row[2] or 0),
            active_hours=int(row[3] or 0),
            peak_ratio=float(row[4] or 0),
            variability=float(row[5] or 0),
            internet_share=float(row[6] or 0),
            feature_timestamp=str(row[7]),
            data_quality_status="PASS",
            feature_freshness="CURRENT"
        )

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Feature data unavailable: {exc}"
        )

    finally:
        if conn is not None:
            conn.close()
# ============================================================
# API5 - RISK PREDICTION CONTRACT
# ============================================================

from pydantic import Field


class RiskPredictionRequest(BaseModel):
    grid_id: int = Field(..., ge=1, le=10000)
    avg_activity: float = Field(..., ge=0)
    activity_growth: float
    active_hours: int = Field(..., ge=0, le=24)
    peak_ratio: float = Field(..., ge=0)
    variability: float = Field(..., ge=0)
    internet_share: float = Field(..., ge=0, le=1)
    feature_timestamp: str


class RiskPredictionResponse(BaseModel):
    grid_id: int
    feature_timestamp: str
    risk_score: float
    risk_level: str
    model_version: str
    explanation_note: str


def get_anomaly_context(grid_id: int, feature_timestamp: str):
    """
    Look up the ML4 anomaly result corresponding to the
    grid and feature timestamp used by the prediction request.
    """
    conn = None

    try:
        conn = get_connection()

        row = conn.execute(
            """
            SELECT
                anomaly_score,
                anomaly_direction,
                ml4_flag,
                reason
            FROM network_anomaly_scores
            WHERE grid_id = ?
              AND timestamp = ?
            LIMIT 1
            """,
            (grid_id, feature_timestamp)
        ).fetchone()

        if row is None:
            return None

        return {
            "anomaly_score": float(row[0]),
            "anomaly_direction": row[1],
            "ml4_flag": int(row[2]),
            "reason": row[3],
        }

    except Exception as exc:
        raise RuntimeError(
            f"ML4 anomaly lookup failed: {exc}"
        )

    finally:
        if conn is not None:
            conn.close()


@app.post(
    "/network/predict-risk",
    response_model=RiskPredictionResponse
)
def predict_risk(request: RiskPredictionRequest):

    # --------------------------------------------------------
    # ML5 - REAL MODEL INFERENCE
    # --------------------------------------------------------

    # Validate that the API contract matches the feature schema
    # used by the trained ML3 model.
    request_features = {
        "avg_activity": request.avg_activity,
        "activity_growth": request.activity_growth,
        "active_hours": request.active_hours,
        "peak_ratio": request.peak_ratio,
        "variability": request.variability,
        "internet_share": request.internet_share,
    }

    missing_features = [
        feature
        for feature in ML_FEATURE_COLUMNS
        if feature not in request_features
    ]

    if missing_features:
        raise HTTPException(
            status_code=500,
            detail=(
                "ML feature schema mismatch. "
                f"Missing features: {missing_features}"
            )
        )

    try:
        feature_frame = pd.DataFrame(
            [[request_features[feature] for feature in ML_FEATURE_COLUMNS]],
            columns=ML_FEATURE_COLUMNS
        )

        # Probability of class 1 = elevated next-hour risk.
        risk_score = float(
            ML_MODEL.predict_proba(feature_frame)[0][1]
        )
        anomaly_context = get_anomaly_context(
            request.grid_id,
            request.feature_timestamp
        )
        if risk_score >= 0.70:
            risk_level = "HIGH"
        elif risk_score >= 0.40:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        return RiskPredictionResponse(
            grid_id=request.grid_id,
            feature_timestamp=request.feature_timestamp,
            risk_score=round(risk_score, 6),
            risk_level=risk_level,
            model_version=MODEL_VERSION,
            explanation_note=(
                "Risk score is the trained ML3 model probability "
                "of elevated high-activity risk for the next hourly "
                "interval. "
                f"ML4 anomaly: {anomaly_context['anomaly_direction']} "
                f"(score={anomaly_context['anomaly_score']:.6f}, "
                f"flag={anomaly_context['ml4_flag']}). "
                f"{anomaly_context['reason']} "
                "This is an operational investigation signal, "
                "not proof of congestion or a network fault."
            )
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"ML5 model inference failed: {exc}"
        )
# ============================================================
# API6 - PIPELINE STATUS
# ============================================================

class PipelineStatus(BaseModel):
    healthy: bool
    run_id: str
    timestamp: str
    status: str
    task_status: dict[str, str]
    rows_in: int
    rows_rejected: int
    nulls_handled: int
    rows_published: int
    as_of: str
    freshness_hours: float
    reasons: list[str]


@app.get("/pipeline/status", response_model=PipelineStatus)
def pipeline_status():

    status_path = BASE_DIR / "data" / "analytics" / "pipeline_status.json"

    if not status_path.exists():
        raise HTTPException(
            status_code=500,
            detail="Pipeline status record not found"
        )

    conn = None

    try:
        with open(status_path) as f:
            status_data = json.load(f)

        conn = get_connection()

        row = conn.execute(
            """
            SELECT
                MAX(timestamp),
                COUNT(*)
            FROM hourly_grid_summary
            """
        ).fetchone()

        as_of = str(row[0])
        rows_published = int(row[1])

        task_status = {
            "ingest": status_data.get("ingest", "unknown"),
            "validate": status_data.get("validate", "unknown"),
            "spark_process": status_data.get("spark_process", "unknown"),
            "load_warehouse": status_data.get("load_warehouse", "unknown"),
            "quality_check": status_data.get("quality_check", "unknown")
        }

        reasons = []

        if status_data.get("status") != "SUCCESS":
            reasons.append(
                f"Pipeline run status is {status_data.get('status')}"
            )

        for task, task_result in task_status.items():
            if task_result != "success":
                reasons.append(
                    f"{task} task status is {task_result}"
                )

        healthy = len(reasons) == 0

        # The current DE7 status record does not contain detailed
        # rejection/null counters, so these are reported as 0
        # rather than inventing values.
        rows_in = rows_published
        rows_rejected = 0
        nulls_handled = 0

        # Current warehouse AS_OF is the analytics freshness point.
        # The source dataset is historical, so freshness is measured
        # relative to the pipeline status timestamp.
        from datetime import datetime

        pipeline_time = datetime.fromisoformat(
            status_data["timestamp"]
        )

        as_of_time = datetime.fromisoformat(
            as_of.replace(" ", "T")
        )

        freshness_hours = abs(
            (pipeline_time - as_of_time).total_seconds()
        ) / 3600

        return PipelineStatus(
            healthy=healthy,
            run_id=str(status_data["run_id"]),
            timestamp=str(status_data["timestamp"]),
            status=str(status_data["status"]),
            task_status=task_status,
            rows_in=rows_in,
            rows_rejected=rows_rejected,
            nulls_handled=nulls_handled,
            rows_published=rows_published,
            as_of=as_of,
            freshness_hours=round(freshness_hours, 2),
            reasons=reasons
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Pipeline status unavailable: {exc}"
        )

    finally:
        if conn is not None:
            conn.close()


# ============================================================
# API6 - GRID LOCATION
# ============================================================

class GridLocation(BaseModel):
    grid_id: int
    centroid_latitude: float
    centroid_longitude: float
    geometry_reference: str


@app.get("/network/grid/{grid_id}/location", response_model=GridLocation)
def grid_location(grid_id: int):

    conn = get_connection()

    try:
        row = conn.execute(
            """
            SELECT
                grid_id,
                centroid_latitude,
                centroid_longitude,
                geometry_reference
            FROM dim_grid
            WHERE grid_id = ?
            """,
            (grid_id,)
        ).fetchone()

        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"Grid {grid_id} not found"
            )

        return GridLocation(
            grid_id=int(row[0]),
            centroid_latitude=float(row[1]),
            centroid_longitude=float(row[2]),
            geometry_reference=str(row[3])
        )

    finally:
        conn.close()
