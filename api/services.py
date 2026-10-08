from .database import get_connection


def get_network_summary(as_of: str | None = None):

    conn = get_connection()

    try:
        # Default AS_OF = latest timestamp in analytics layer
        if as_of is None:
            row = conn.execute(
                "SELECT MAX(timestamp) FROM hourly_grid_summary"
            ).fetchone()

            if not row or row[0] is None:
                raise RuntimeError("Analytics layer is empty")

            effective_as_of = row[0]
        else:
            effective_as_of = as_of

        # Everything needed for the summary is calculated in one query.
        row = conn.execute(
            """
            WITH summary AS (
                SELECT
                    SUM(total_activity) AS total_activity,
                    COUNT(DISTINCT CASE
                        WHEN total_activity > 0 THEN grid_id
                    END) AS active_grids
                FROM hourly_grid_summary
                WHERE timestamp = ?
            ),
            peak AS (
                SELECT timestamp
                FROM hourly_grid_summary
                GROUP BY timestamp
                ORDER BY SUM(total_activity) DESC, timestamp ASC
                LIMIT 1
            ),
            top_grid AS (
                SELECT grid_id
                FROM hourly_grid_summary
                WHERE timestamp = ?
                GROUP BY grid_id
                ORDER BY SUM(total_activity) DESC, grid_id ASC
                LIMIT 1
            )
            SELECT
                summary.total_activity,
                summary.active_grids,
                peak.timestamp,
                top_grid.grid_id
            FROM summary
            CROSS JOIN peak
            CROSS JOIN top_grid
            """,
            (effective_as_of, effective_as_of)
        ).fetchone()

        if row is None:
            raise RuntimeError(
                "No analytics data available for requested AS_OF"
            )

        return {
            "total_activity": float(row[0] or 0),
            "active_grids": int(row[1] or 0),
            "peak_hour": str(row[2]),
            "top_grid": int(row[3]),
            "as_of": str(effective_as_of),
        }

    finally:
        conn.close()
