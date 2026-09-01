import pandas as pd
import logging
from pathlib import Path


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    filename="np1_execution.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# USAGE PROCESSOR
# ============================================================

class UsageProcessor:

    REQUIRED_COLUMNS = [
        "timestamp",
        "grid_id",
        "country_code",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet"
    ]

    ACTIVITY_COLUMNS = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet"
    ]

    def __init__(self, file_path=None, df=None):
        self.file_path = file_path
        self.df = df
        self.grid_hourly = None
        self.daily_summary = None
        self.grid_summary = None

    # ========================================================
    # 1. LOAD DATA
    # ========================================================

    def load_data(self):

        if self.df is None:

            if self.file_path is None:
                raise ValueError("Either file_path or DataFrame is required.")

            self.df = pd.read_csv(self.file_path)

        logger.info(
            "Loaded %d rows and %d columns",
            self.df.shape[0],
            self.df.shape[1]
        )

        # Rename raw CSV columns
        self.df = self.df.rename(columns={
            "datetime": "timestamp",
            "CellID": "grid_id",
            "countrycode": "country_code",
            "smsin": "sms_in",
            "smsout": "sms_out",
            "callin": "call_in",
            "callout": "call_out",
            "internet": "internet"
        })

        return self.df

    # ========================================================
    # 2. CLEAN DATA
    # ========================================================

    def clean_data(self):

        missing = [
            col for col in self.REQUIRED_COLUMNS
            if col not in self.df.columns
        ]

        if missing:
            raise ValueError(
                f"Missing required columns: {missing}"
            )

        # Convert timestamp
        self.df["timestamp"] = pd.to_datetime(
            self.df["timestamp"],
            errors="coerce"
        )

        # Convert activity columns to numeric
        for col in self.ACTIVITY_COLUMNS:
            self.df[col] = pd.to_numeric(
                self.df[col],
                errors="coerce"
            )

        before = len(self.df)

        # Required fields cannot be null
        self.df = self.df.dropna(
            subset=["timestamp", "grid_id"]
        )

        # Negative activity values are invalid
        negative_mask = (
            self.df[self.ACTIVITY_COLUMNS] < 0
        ).any(axis=1)

        negative_count = negative_mask.sum()

        self.df = self.df[~negative_mask]

        # Curated-layer null handling:
        # missing activity values are treated as zero
        self.df[self.ACTIVITY_COLUMNS] = (
            self.df[self.ACTIVITY_COLUMNS].fillna(0)
        )

        dropped = before - len(self.df)

        logger.info(
            "Dropped %d invalid rows (%d negative-activity rows)",
            dropped,
            negative_count
        )

        logger.info(
            "Clean rows remaining: %d",
            len(self.df)
        )

        return self.df

    # ========================================================
    # 3. TIME FEATURES
    # ========================================================

    def derive_time_features(self):

        self.df["date"] = self.df["timestamp"].dt.date
        self.df["hour"] = self.df["timestamp"].dt.hour
        self.df["day_of_week"] = (
            self.df["timestamp"].dt.day_name()
        )

        return self.df

    # ========================================================
    # 4. GRID + HOUR AGGREGATION
    # ========================================================

    def aggregate_to_grid_time(self):

        # Preserve only analytics columns.
        # country_code is intentionally excluded.
        self.grid_hourly = (
            self.df.groupby(
                ["timestamp", "grid_id"],
                as_index=False
            )[self.ACTIVITY_COLUMNS]
            .sum()
        )

        return self.grid_hourly

    # ========================================================
    # 5. ACTIVITY FEATURES
    # ========================================================

    def derive_activity_features(self):

        if self.grid_hourly is None:
            raise ValueError(
                "Run aggregate_to_grid_time() first."
            )

        self.grid_hourly["total_sms"] = (
            self.grid_hourly["sms_in"]
            + self.grid_hourly["sms_out"]
        )

        self.grid_hourly["total_calls"] = (
            self.grid_hourly["call_in"]
            + self.grid_hourly["call_out"]
        )

        self.grid_hourly["total_activity"] = (
            self.grid_hourly["total_sms"]
            + self.grid_hourly["total_calls"]
            + self.grid_hourly["internet"]
        )

        return self.grid_hourly

    # ========================================================
    # 6. KPIs / SUMMARY TABLES
    # ========================================================

    def compute_kpis(self):

        if self.grid_hourly is None:
            raise ValueError(
                "Run aggregation first."
            )

        # Daily summary
        self.daily_summary = (
            self.grid_hourly
            .assign(
                date=pd.to_datetime(
                    self.grid_hourly["timestamp"]
                ).dt.date
            )
            .groupby("date", as_index=False)
            .agg(
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                total_internet=("internet", "sum"),
                total_activity=("total_activity", "sum"),
                active_grids=("grid_id", "nunique")
            )
        )

        # Grid summary
        self.grid_summary = (
            self.grid_hourly
            .groupby("grid_id", as_index=False)
            .agg(
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                total_internet=("internet", "sum"),
                total_activity=("total_activity", "sum"),
                active_hours=("timestamp", "nunique")
            )
        )

        return self.daily_summary, self.grid_summary

    # ========================================================
    # 7. EXPORT
    # ========================================================

    def export_summary(self, output_dir="."):

        output_dir = Path(output_dir)
        output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        daily_path = output_dir / "daily_summary.csv"
        grid_path = output_dir / "grid_summary.csv"

        self.daily_summary.to_csv(
            daily_path,
            index=False
        )

        self.grid_summary.to_csv(
            grid_path,
            index=False
        )

        # Also export grid/hour analytics
        hourly_path = (
            output_dir / "grid_hourly_analytics.csv"
        )

        self.grid_hourly.to_csv(
            hourly_path,
            index=False
        )

        logger.info(
            "Daily summary exported to %s",
            daily_path
        )

        logger.info(
            "Grid summary exported to %s",
            grid_path
        )

        logger.info(
            "Grid/hour analytics exported to %s",
            hourly_path
        )

        return daily_path, grid_path


# ============================================================
# UNIT-STYLE VALIDATIONS
# ============================================================

def validate_load(processor):
    assert processor.df is not None
    assert len(processor.df) > 0
    logger.info("Validation passed: load_data()")


def validate_clean(processor):
    assert "timestamp" in processor.df.columns
    assert "grid_id" in processor.df.columns
    assert not (
        processor.df[processor.ACTIVITY_COLUMNS] < 0
    ).any().any()

    logger.info("Validation passed: clean_data()")


def validate_time_features(processor):
    assert "date" in processor.df.columns
    assert "hour" in processor.df.columns
    assert "day_of_week" in processor.df.columns

    logger.info(
        "Validation passed: derive_time_features()"
    )


def validate_aggregation(processor):
    assert processor.grid_hourly is not None
    assert "grid_id" in processor.grid_hourly.columns
    assert "timestamp" in processor.grid_hourly.columns

    logger.info(
        "Validation passed: aggregate_to_grid_time()"
    )


def validate_activity_features(processor):
    required = [
        "total_sms",
        "total_calls",
        "total_activity"
    ]

    for col in required:
        assert col in processor.grid_hourly.columns

    logger.info(
        "Validation passed: derive_activity_features()"
    )


def validate_kpis(processor):
    assert processor.daily_summary is not None
    assert processor.grid_summary is not None

    logger.info(
        "Validation passed: compute_kpis()"
    )


# ============================================================
# MAIN PIPELINE
# ============================================================

if __name__ == "__main__":

    file_path = (
        r"C:\Users\bhavani.as\Desktop\NP1"
        r"\sms-call-internet-mi-2013-11-01.csv"
    )

    processor = UsageProcessor(
        file_path=file_path
    )

    # Run pipeline
    processor.load_data()
    validate_load(processor)

    processor.clean_data()
    validate_clean(processor)

    processor.derive_time_features()
    validate_time_features(processor)

    processor.aggregate_to_grid_time()
    validate_aggregation(processor)

    processor.derive_activity_features()
    validate_activity_features(processor)

    processor.compute_kpis()
    validate_kpis(processor)

    processor.export_summary()

    logger.info(
        "NP1 UsageProcessor pipeline completed successfully."
    )

    print("NP1 processing completed successfully.")
    print("Created:")
    print("- daily_summary.csv")
    print("- grid_summary.csv")
    print("- grid_hourly_analytics.csv")
    print("- np1_execution.log")