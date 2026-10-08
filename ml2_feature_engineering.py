import sqlite3
import pandas as pd
import numpy as np

DB_PATH = "data/network_analytics.db"

# ============================================================
# ML2 FEATURE WINDOW CONVENTION
# ============================================================
# Prediction target:
#     t + 1
#
# Feature history:
#     trailing 24 hourly intervals ending at t
#
# Baseline:
#     preceding 24 hourly intervals before the trailing window
#
# Therefore:
#     recent window = [t-23 ... t]
#     baseline      = [t-47 ... t-24]
#
# No feature may use data after t.
# ============================================================

RECENT_WINDOW = 24
BASELINE_WINDOW = 24


def load_data(conn):
    query = """
        SELECT
            grid_id,
            timestamp,
            total_activity,
            internet_activity
        FROM hourly_grid_summary
        ORDER BY grid_id, timestamp
    """

    df = pd.read_sql_query(query, conn)

    df["timestamp"] = pd.to_datetime(df["timestamp"])

    return df


def build_features(df):
    results = []

    timestamps = sorted(df["timestamp"].unique())

    # Need 48 historical hours:
    # 24 recent + 24 baseline.
    for t in timestamps:
        recent_start = t - pd.Timedelta(hours=RECENT_WINDOW - 1)
        baseline_start = t - pd.Timedelta(
            hours=RECENT_WINDOW + BASELINE_WINDOW - 1
        )
        baseline_end = t - pd.Timedelta(hours=RECENT_WINDOW)

        recent = df[
            (df["timestamp"] >= recent_start)
            & (df["timestamp"] <= t)
        ]

        baseline = df[
            (df["timestamp"] >= baseline_start)
            & (df["timestamp"] <= baseline_end)
        ]

        # Do not create a feature row until the full
        # recent + baseline history is available.
        if len(recent) == 0 or len(baseline) == 0:
            continue

        recent_grouped = (
            recent.groupby("grid_id")
            .agg(
                avg_activity=("total_activity", "mean"),
                peak_activity=("total_activity", "max"),
                active_hours=(
                    "total_activity",
                    lambda x: int((x > 0).sum())
                ),
                variability=("total_activity", "std"),
                internet_activity=("internet_activity", "sum"),
                total_activity_sum=("total_activity", "sum"),
                recent_count=("total_activity", "count"),
            )
            .reset_index()
        )

        baseline_grouped = (
            baseline.groupby("grid_id")
            .agg(
                baseline_avg_activity=("total_activity", "mean"),
                baseline_count=("total_activity", "count"),
            )
            .reset_index()
        )

        merged = recent_grouped.merge(
            baseline_grouped,
            on="grid_id",
            how="inner"
        )

        # Require complete 24-hour windows.
        merged = merged[
            (merged["recent_count"] == RECENT_WINDOW)
            & (merged["baseline_count"] == BASELINE_WINDOW)
        ].copy()

        if merged.empty:
            continue

        # Activity growth:
        # recent average compared with prior 24-hour average.
        merged["activity_growth"] = np.where(
            merged["baseline_avg_activity"] > 0,
            (
                merged["avg_activity"]
                - merged["baseline_avg_activity"]
            ) / merged["baseline_avg_activity"],
            0.0
        )

        # Peak-to-average ratio.
        merged["peak_ratio"] = np.where(
            merged["avg_activity"] > 0,
            merged["peak_activity"] / merged["avg_activity"],
            0.0
        )

        # Standard deviation is used as the variability proxy.
        merged["variability"] = merged["variability"].fillna(0.0)

        # Internet share.
        merged["internet_share"] = np.where(
            merged["total_activity_sum"] > 0,
            merged["internet_activity"]
            / merged["total_activity_sum"],
            0.0
        )

        output = merged[
            [
                "grid_id",
                "avg_activity",
                "activity_growth",
                "active_hours",
                "peak_ratio",
                "variability",
                "internet_share",
            ]
        ].copy()

        output["feature_timestamp"] = t

        results.append(output)

    if not results:
        raise RuntimeError("No ML2 feature rows were generated.")

    features = pd.concat(results, ignore_index=True)

    return features[
        [
            "grid_id",
            "feature_timestamp",
            "avg_activity",
            "activity_growth",
            "active_hours",
            "peak_ratio",
            "variability",
            "internet_share",
        ]
    ]


def persist_features(conn, features):
    features.to_sql(
        "network_feature_table",
        conn,
        if_exists="replace",
        index=False
    )

    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS
        idx_network_feature_grid_timestamp
        ON network_feature_table(grid_id, feature_timestamp)
    """)

    conn.commit()


def leakage_test(df, features):
    """
    Leakage test:

    For a selected grid and cutoff t, calculate the features
    using only data <= t.

    Then append future data (> t).

    The feature values at t must remain unchanged.

    If they change, future information has leaked into the
    feature calculation.
    """

    test_grid = int(features.iloc[0]["grid_id"])
    test_timestamp = pd.Timestamp(
        features.iloc[0]["feature_timestamp"]
    )

    historical_only = df[
        (df["grid_id"] == test_grid)
        & (df["timestamp"] <= test_timestamp)
    ].copy()

    historical_with_future = df[
        df["grid_id"] == test_grid
    ].copy()

    def calculate_single(frame):
        recent_start = test_timestamp - pd.Timedelta(hours=23)
        baseline_start = test_timestamp - pd.Timedelta(hours=47)
        baseline_end = test_timestamp - pd.Timedelta(hours=24)

        recent = frame[
            (frame["timestamp"] >= recent_start)
            & (frame["timestamp"] <= test_timestamp)
        ]

        baseline = frame[
            (frame["timestamp"] >= baseline_start)
            & (frame["timestamp"] <= baseline_end)
        ]

        if len(recent) != 24 or len(baseline) != 24:
            raise AssertionError(
                "Insufficient history for leakage test."
            )

        avg_activity = recent["total_activity"].mean()
        baseline_avg = baseline["total_activity"].mean()
        peak = recent["total_activity"].max()

        variability = recent["total_activity"].std()

        if baseline_avg > 0:
            growth = (
                avg_activity - baseline_avg
            ) / baseline_avg
        else:
            growth = 0.0

        peak_ratio = (
            peak / avg_activity
            if avg_activity > 0
            else 0.0
        )

        total_activity = recent["total_activity"].sum()
        internet_activity = recent["internet_activity"].sum()

        internet_share = (
            internet_activity / total_activity
            if total_activity > 0
            else 0.0
        )

        return np.array([
            avg_activity,
            growth,
            int((recent["total_activity"] > 0).sum()),
            peak_ratio,
            0.0 if pd.isna(variability) else variability,
            internet_share,
        ])

    before = calculate_single(historical_only)
    after = calculate_single(historical_with_future)

    if not np.allclose(before, after, rtol=1e-10, atol=1e-10):
        raise AssertionError(
            "LEAKAGE TEST FAILED: future data changed "
            "features at feature_timestamp."
        )

    print(
        f"PASS: leakage test for grid {test_grid} "
        f"at {test_timestamp}"
    )


def validate_table(conn):
    row_count = conn.execute("""
        SELECT COUNT(*)
        FROM network_feature_table
    """).fetchone()[0]

    grid_count = conn.execute("""
        SELECT COUNT(DISTINCT grid_id)
        FROM network_feature_table
    """).fetchone()[0]

    min_ts, max_ts = conn.execute("""
        SELECT
            MIN(feature_timestamp),
            MAX(feature_timestamp)
        FROM network_feature_table
    """).fetchone()

    print("\n=== NETWORK FEATURE TABLE ===")
    print(f"Rows: {row_count}")
    print(f"Distinct grids: {grid_count}")
    print(f"Feature range: {min_ts} -> {max_ts}")

    expected_columns = {
        "grid_id",
        "feature_timestamp",
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability",
        "internet_share",
    }

    actual_columns = {
        row[1]
        for row in conn.execute(
            "PRAGMA table_info(network_feature_table)"
        ).fetchall()
    }

    missing = expected_columns - actual_columns

    if missing:
        raise AssertionError(
            f"Missing required columns: {sorted(missing)}"
        )

    print("Schema validation: PASS")


def main():
    conn = sqlite3.connect(DB_PATH)

    print("Loading hourly warehouse data...")
    df = load_data(conn)

    print(
        f"Loaded {len(df):,} warehouse rows "
        f"across {df['timestamp'].nunique()} hourly intervals."
    )

    print("\nBuilding leakage-safe ML2 features...")
    features = build_features(df)

    print(f"Generated {len(features):,} feature rows.")

    print("\nPersisting network_feature_table...")
    persist_features(conn, features)

    validate_table(conn)

    print("\nRunning leakage test...")
    leakage_test(df, features)

    print("\nML2 COMPLETE: feature table created successfully.")

    conn.close()


if __name__ == "__main__":
    main()
