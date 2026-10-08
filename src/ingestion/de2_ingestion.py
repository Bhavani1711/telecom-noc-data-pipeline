
from pathlib import Path
from datetime import datetime
import shutil
import csv


# ============================================================
# DE2 - LANDING TO RAW INGESTION
# ============================================================

print("\n==============================================")
print("        DE2 INGESTION PROGRAM STARTED")
print("==============================================")


# ============================================================
# PROJECT LOCATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]


# ============================================================
# DATA LOCATIONS
# ============================================================

LANDING_DIR = BASE_DIR / "data" / "landing" / "raw_csv_files"
RAW_DIR = BASE_DIR / "data" / "raw"
REJECTED_DIR = BASE_DIR / "data" / "rejected"
REFERENCE_DIR = BASE_DIR / "data" / "reference"
LOG_DIR = BASE_DIR / "logs"

METADATA_FILE = LOG_DIR / "ingestion_metadata.csv"


# ============================================================
# REQUIRED COLUMNS
# ============================================================

REQUIRED_COLUMNS = [
    "datetime",
    "CellID",
    "countrycode",
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet"
]

ACTIVITY_COLUMNS = [
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet"
]


# ============================================================
# 1. DETECT FILES
# ============================================================

def detect_files():

    print("\n----------------------------------------------")
    print("1. DETECT FILES")
    print("----------------------------------------------")

    files = sorted(
        LANDING_DIR.glob("sms-call-internet-mi-*.csv")
    )

    print(f"Detected {len(files)} daily CSV file(s).")

    for file in files:
        print(f"  -> {file.name}")

    return files


# ============================================================
# 2. VALIDATE SCHEMA
# ============================================================

def validate_schema(file_path):

    print("\n----------------------------------------------")
    print("2. VALIDATE SCHEMA")
    print("----------------------------------------------")

    try:

        with open(
            file_path,
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as file:

            reader = csv.reader(file)
            header = next(reader)

        header = [
            column.strip()
            for column in header
        ]

        columns = set(header)

        # FIX: convert REQUIRED_COLUMNS list to a set
        missing_columns = set(REQUIRED_COLUMNS) - columns

        if missing_columns:

            reason = (
                "Missing required column(s): "
                + ", ".join(sorted(missing_columns))
            )

            print("FAILED")
            print(reason)

            return False, reason

        print("PASSED - all required columns exist")

        return True, "Schema validation passed"

    except Exception as error:

        reason = f"Schema validation error: {error}"

        print("FAILED")
        print(reason)

        return False, reason


# ============================================================
# 3. VALIDATE MINIMUM QUALITY
# ============================================================

def validate_minimum_quality(file_path):

    print("\n----------------------------------------------")
    print("3. VALIDATE MINIMUM QUALITY")
    print("----------------------------------------------")

    row_count = 0

    try:

        with open(
            file_path,
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as file:

            reader = csv.DictReader(file)

            for row in reader:

                row_count += 1

                # --------------------------------------------
                # Check activity columns
                # --------------------------------------------

                for column in ACTIVITY_COLUMNS:

                    value_text = row.get(column)

                    # Empty/blank activity values are allowed
                    # because the real dataset contains NaN values.
                    if value_text is None or value_text.strip() == "":
                        continue

                    try:

                        value = float(value_text)

                    except ValueError:

                        reason = (
                            f"Invalid numeric value in {column}"
                        )

                        print("FAILED")
                        print(reason)

                        return False, row_count, reason

                    # Negative activity is NOT allowed
                    if value < 0:

                        reason = (
                            f"Negative activity value in {column}"
                        )

                        print("FAILED")
                        print(reason)

                        return False, row_count, reason

        if row_count == 0:

            reason = "File contains no data rows"

            print("FAILED")
            print(reason)

            return False, row_count, reason

        print(f"Rows checked: {row_count}")
        print("PASSED - minimum quality checks passed")

        return True, row_count, "Minimum quality validation passed"

    except Exception as error:

        reason = (
            f"Minimum quality validation error: {error}"
        )

        print("FAILED")
        print(reason)

        return False, row_count, reason


# ============================================================
# 4. ROUTE FILE
# ============================================================

def route_file(file_path, valid, reason):

    print("\n----------------------------------------------")
    print("4. ROUTE FILE")
    print("----------------------------------------------")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)

    if valid:

        destination = RAW_DIR / file_path.name
        status = "VALID"

    else:

        destination = REJECTED_DIR / file_path.name
        status = "REJECTED"

    if destination.exists():
        destination.unlink()

    shutil.move(
        str(file_path),
        str(destination)
    )

    print(f"File: {file_path.name}")
    print(f"Status: {status}")
    print(f"Reason: {reason}")
    print(f"Moved to: {destination}")

    return status


# ============================================================
# 5. WRITE METADATA
# ============================================================

def write_metadata(
    filename,
    status,
    row_count,
    reason
):

    print("\n----------------------------------------------")
    print("5. WRITE METADATA")
    print("----------------------------------------------")

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    metadata_exists = METADATA_FILE.exists()

    with open(
        METADATA_FILE,
        "a",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        if not metadata_exists:

            writer.writerow([
                "filename",
                "status",
                "row_count",
                "reason",
                "processed_at"
            ])

        writer.writerow([
            filename,
            status,
            row_count,
            reason,
            datetime.now().isoformat()
        ])

    print(f"Metadata saved to: {METADATA_FILE}")


# ============================================================
# 6. PROCESS ONE FILE
# ============================================================

def process_file(file_path):

    print("\n==============================================")
    print(f"PROCESSING: {file_path.name}")
    print("==============================================")

    # Count rows
    try:

        with open(
            file_path,
            "r",
            encoding="utf-8-sig"
        ) as file:

            row_count = max(
                sum(1 for _ in file) - 1,
                0
            )

    except Exception:

        row_count = 0

    print(f"Initial row count: {row_count}")

    # --------------------------------------------------------
    # Schema validation
    # --------------------------------------------------------

    schema_valid, schema_reason = validate_schema(file_path)

    if not schema_valid:

        status = route_file(
            file_path,
            False,
            schema_reason
        )

        write_metadata(
            file_path.name,
            status,
            row_count,
            schema_reason
        )

        return

    # --------------------------------------------------------
    # Minimum quality validation
    # --------------------------------------------------------

    quality_valid, quality_row_count, quality_reason = (
        validate_minimum_quality(file_path)
    )

    row_count = quality_row_count

    if not quality_valid:

        status = route_file(
            file_path,
            False,
            quality_reason
        )

        write_metadata(
            file_path.name,
            status,
            row_count,
            quality_reason
        )

        return

    # --------------------------------------------------------
    # Everything passed
    # --------------------------------------------------------

    final_reason = "All validation checks passed"

    status = route_file(
        file_path,
        True,
        final_reason
    )

    write_metadata(
        file_path.name,
        status,
        row_count,
        final_reason
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("\n==============================================")
    print("DE2 LANDING -> RAW PIPELINE")
    print("==============================================")

    # Create folders
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    print("\nIMPORTANT LOCATIONS")
    print(f"Landing  : {LANDING_DIR}")
    print(f"Raw      : {RAW_DIR}")
    print(f"Rejected : {REJECTED_DIR}")
    print(f"Reference: {REFERENCE_DIR}")
    print(f"Logs     : {LOG_DIR}")

    # --------------------------------------------------------
    # Check reference file
    # --------------------------------------------------------

    geojson_file = REFERENCE_DIR / "milano-grid.geojson"

    print("\nReference data:")

    if geojson_file.exists():

        print("OK - milano-grid.geojson found")

    else:

        print("WARNING - milano-grid.geojson not found")

    # --------------------------------------------------------
    # Detect files
    # --------------------------------------------------------

    files = detect_files()

    if not files:

        print("\nNO FILES TO PROCESS.")
        return

    # --------------------------------------------------------
    # Process files
    # --------------------------------------------------------

    for file_path in files:

        process_file(file_path)

    print("\n==============================================")
    print("        DE2 INGESTION COMPLETED")
    print("==============================================")

    print("\nCheck:")
    print(f"RAW      : {RAW_DIR}")
    print(f"REJECTED : {REJECTED_DIR}")
    print(f"LOG      : {METADATA_FILE}")


# ============================================================
# START PROGRAM
# ============================================================

if __name__ == "__main__":
    main()
