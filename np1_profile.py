import pandas as pd
import numpy as np
from pathlib import Path

# ============================================================
# 1. LOAD DATASET
# ============================================================

file_path = r"C:\Users\bhavani.as\Desktop\NP1\sms-call-internet-mi-2013-11-01.csv"

df = pd.read_csv(file_path)

print("Shape:", df.shape)

print("\nRaw columns:")
print(df.columns.tolist())

print("\nData types:")
print(df.dtypes)


# ============================================================
# 2. RENAME COLUMNS
# ============================================================

df = df.rename(columns={
    "datetime": "timestamp",
    "CellID": "grid_id",
    "countrycode": "country_code",
    "smsin": "sms_in",
    "smsout": "sms_out",
    "callin": "call_in",
    "callout": "call_out",
    "internet": "internet"
})

print("\nRenamed columns:")
print(df.columns.tolist())


# ============================================================
# 3. TIMESTAMP PROCESSING
# ============================================================

df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    errors="coerce"
)

df["date"] = df["timestamp"].dt.date

df["hour"] = df["timestamp"].dt.hour

df["day_of_week"] = df["timestamp"].dt.day_name()


# ============================================================
# 4. TIMESTAMP / TIME-GRAIN CHECK
# ============================================================

timestamps = (
    df["timestamp"]
    .dropna()
    .drop_duplicates()
    .sort_values()
)

print("\nNumber of distinct timestamps:", len(timestamps))

intervals = timestamps.diff().dropna()

print("\nTime intervals:")
print(intervals.value_counts())

print(
    "\nAll intervals exactly 1 hour:",
    (intervals == pd.Timedelta(hours=1)).all()
)

print("\nTime range:")
print(timestamps.min(), "to", timestamps.max())


# ============================================================
# 5. ACTIVITY COLUMNS
# ============================================================

activity_cols = [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet"
]


# ============================================================
# 6. MISSING VALUES
# ============================================================

print("\nMissing values:")
print(df.isnull().sum())


# ============================================================
# 7. BLANK ACTIVITY VALUES
# ============================================================

print("\nBlank activity values:")

for col in activity_cols:
    print(
        col,
        (df[col].astype(str).str.strip() == "").sum()
    )


# ============================================================
# 8. DUPLICATE ROWS
# ============================================================

print("\nExact duplicate rows:", df.duplicated().sum())


# ============================================================
# 9. NEGATIVE VALUES
# ============================================================

print("\nNegative values:")

for col in activity_cols:

    numeric_col = pd.to_numeric(
        df[col],
        errors="coerce"
    )

    print(
        col,
        (numeric_col < 0).sum()
    )


# ============================================================
# 10. MISSING KEY FIELDS
# ============================================================

print("\nMissing grid_id:", df["grid_id"].isna().sum())

print(
    "Missing timestamp:",
    df["timestamp"].isna().sum()
)


# ============================================================
# 11. GRAIN CHECK
# ============================================================

grain_counts = (
    df.groupby(
        ["timestamp", "grid_id"]
    )["country_code"]
    .nunique()
)

print("\nCountry-code rows per grid/hour:")
print(
    grain_counts
    .value_counts()
    .sort_index()
)


print(
    "\nRaw grain duplicate combinations:",
    df.duplicated(
        subset=[
            "timestamp",
            "grid_id",
            "country_code"
        ]
    ).sum()
)


# ============================================================
# 12. CONVERT ACTIVITY COLUMNS TO NUMERIC
# ============================================================

for col in activity_cols:

    df[col] = pd.to_numeric(
        df[col],
        errors="coerce"
    )


# ============================================================
# 13. CREATE ACTIVITY FEATURES
# ============================================================

df["total_sms"] = (
    df["sms_in"] + df["sms_out"]
)

df["total_calls"] = (
    df["call_in"] + df["call_out"]
)

df["total_activity"] = (
    df["total_sms"]
    + df["total_calls"]
    + df["internet"]
)


print("\nActivity totals preview:")

print(
    df[
        [
            "total_sms",
            "total_calls",
            "total_activity"
        ]
    ].head()
)


# ============================================================
# 14. NP1 PROFILE SUMMARY
# ============================================================

print("\n========== NP1 PROFILE ==========")

print(
    "Unique grids:",
    df["grid_id"].nunique()
)

print("\nTime range:")

print(
    df["timestamp"].min(),
    "to",
    df["timestamp"].max()
)

print(
    "Distinct timestamps:",
    df["timestamp"].nunique()
)


# ============================================================
# 15. COUNTRY CODE ANALYSIS
# ============================================================

print("\nCountry-code categories:")

print(
    df["country_code"].unique()
)

print("\nCountry-code counts:")

print(
    df["country_code"].value_counts()
)


# ============================================================
# 16. NULL COUNTS AFTER PROCESSING
# ============================================================

print("\nNull counts:")

print(
    df.isnull().sum()
)


# ============================================================
# 17. BUSIEST HOUR
# ============================================================

print("\nBusiest hour:")

print(
    df.groupby("hour")["total_activity"]
    .sum()
    .sort_values(ascending=False)
    .head(1)
)


# ============================================================
# 18. BUSIEST GRID
# ============================================================

print("\nBusiest grid:")

print(
    df.groupby("grid_id")["total_activity"]
    .sum()
    .sort_values(ascending=False)
    .head(1)
)


print("\n========== PROFILE COMPLETE ==========")