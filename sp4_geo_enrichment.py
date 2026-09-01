import os
import sys
import glob
import json


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_DIR = r"C:\Users\bhavani.as\Desktop\NP1"

OUTPUT_DIR = os.path.join(
    PROJECT_DIR,
    "sp4_output"
)

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)


# ============================================================
# WINDOWS PYSPARK PYTHON FIX
# ============================================================

# Your actual Python 3.10 executable
PYTHON_EXE = r"C:\Users\bhavani.as\AppData\Local\Programs\Python\Python310\python.exe"

# Force PySpark driver and worker processes
# to use the same Python executable.
os.environ["PYSPARK_PYTHON"] = PYTHON_EXE
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_EXE


# ============================================================
# IMPORT PYSPARK
# ============================================================

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    desc,
    broadcast
)


# ============================================================
# SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("NP1 SP4 GeoJSON Enrichment")
    .master("local[*]")
    .config(
        "spark.pyspark.python",
        PYTHON_EXE
    )
    .config(
        "spark.pyspark.driver.python",
        PYTHON_EXE
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 75)
print("SP4 - TELECOM ACTIVITY + MILAN GRID GEOJSON ENRICHMENT")
print("=" * 75)

print(
    "Python executable:",
    PYTHON_EXE
)

print(
    "Spark version:",
    spark.version
)

print(
    "Project directory:",
    PROJECT_DIR
)


# ============================================================
# HELPER: FIND SP3 OUTPUT
# ============================================================

def find_sp3_output():

    candidates = []

    # Direct expected location
    direct_folder = os.path.join(
        PROJECT_DIR,
        "sp3_output",
        "hourly_grid_summary"
    )

    if os.path.isdir(direct_folder):

        csv_files = glob.glob(
            os.path.join(
                direct_folder,
                "*.csv"
            )
        )

        candidates.extend(csv_files)

    # Recursive fallback
    if not candidates:

        pattern = os.path.join(
            PROJECT_DIR,
            "**",
            "hourly_grid_summary",
            "*.csv"
        )

        candidates = glob.glob(
            pattern,
            recursive=True
        )

    # Another fallback
    if not candidates:

        pattern = os.path.join(
            PROJECT_DIR,
            "**",
            "*hourly_grid_summary*.csv"
        )

        candidates = glob.glob(
            pattern,
            recursive=True
        )

    return candidates


# ============================================================
# FIND SP3 OUTPUT
# ============================================================

print()
print("=" * 75)
print("SEARCHING FOR SP3 OUTPUT")
print("=" * 75)


sp3_files = find_sp3_output()


if not sp3_files:

    print()
    print("ERROR: SP3 hourly_grid_summary was not found.")

    print()
    print("Expected location:")
    print(
        os.path.join(
            PROJECT_DIR,
            "sp3_output",
            "hourly_grid_summary"
        )
    )

    spark.stop()
    raise SystemExit(1)


print()
print("SP3 OUTPUT FOUND:")

for i, file in enumerate(
    sp3_files,
    1
):

    print(
        f"{i}. {file}"
    )


SP3_FILE = sp3_files[0]

print()
print("Using SP3 file:")
print(SP3_FILE)


# ============================================================
# FIND GEOJSON
# ============================================================

print()
print("=" * 75)
print("SEARCHING FOR MILAN GEOJSON")
print("=" * 75)


def find_geojson_files():

    search_roots = [
        PROJECT_DIR,
        os.path.join(
            os.path.expanduser("~"),
            "Desktop"
        ),
        os.path.join(
            os.path.expanduser("~"),
            "Downloads"
        ),
        os.path.join(
            os.path.expanduser("~"),
            "Documents"
        )
    ]

    found = []

    for root in search_roots:

        if not os.path.exists(root):
            continue

        pattern = os.path.join(
            root,
            "**",
            "*"
        )

        try:

            all_files = glob.glob(
                pattern,
                recursive=True
            )

        except Exception:
            continue

        for file in all_files:

            if not os.path.isfile(file):
                continue

            filename = os.path.basename(
                file
            ).lower()

            if (
                filename.endswith(".geojson")
                or filename.endswith(".json")
            ):

                if file not in found:
                    found.append(file)

    return found


all_geojson_files = find_geojson_files()


print()
print("GeoJSON/JSON files discovered:")

if all_geojson_files:

    for i, file in enumerate(
        all_geojson_files,
        1
    ):

        print(
            f"{i}. {file}"
        )

else:

    print(
        "NO .geojson or .json files were discovered."
    )


# ============================================================
# IDENTIFY MILAN GRID FILE
# ============================================================

geojson_candidates = []


for file in all_geojson_files:

    filename = os.path.basename(
        file
    ).lower()

    # Highest priority:
    # milano-grid.geojson
    if filename == "milano-grid.geojson":

        geojson_candidates.insert(
            0,
            file
        )

    # Other likely names
    elif (
        "milano" in filename
        and "grid" in filename
    ):

        geojson_candidates.append(
            file
        )

    elif (
        "milan" in filename
        and "grid" in filename
    ):

        geojson_candidates.append(
            file
        )


# Remove duplicates while preserving order
geojson_candidates = list(
    dict.fromkeys(
        geojson_candidates
    )
)


# ============================================================
# IF NOT FOUND BY NAME, INSPECT JSON CONTENT
# ============================================================

if not geojson_candidates:

    print()
    print(
        "Exact filename was not found."
    )

    print(
        "Checking JSON files for a GeoJSON "
        "FeatureCollection..."
    )

    for file in all_geojson_files:

        try:

            with open(
                file,
                "r",
                encoding="utf-8"
            ) as f:

                data = json.load(f)

            if (
                isinstance(data, dict)
                and data.get("type")
                == "FeatureCollection"
                and isinstance(
                    data.get("features"),
                    list
                )
                and len(
                    data.get("features")
                ) > 0
            ):

                # Check whether features contain cellId
                first_feature = data[
                    "features"
                ][0]

                properties = (
                    first_feature.get(
                        "properties",
                        {}
                    )
                )

                if (
                    "cellId"
                    in properties
                ):

                    geojson_candidates.append(
                        file
                    )

        except Exception:
            continue


# ============================================================
# FINAL GEOJSON CHECK
# ============================================================

if not geojson_candidates:

    print()
    print("=" * 75)
    print("ERROR: MILAN GRID GEOJSON COULD NOT BE FOUND")
    print("=" * 75)

    print()
    print("The script searched:")

    print(
        "1.",
        PROJECT_DIR
    )

    print(
        "2.",
        os.path.join(
            os.path.expanduser("~"),
            "Desktop"
        )
    )

    print(
        "3.",
        os.path.join(
            os.path.expanduser("~"),
            "Downloads"
        )
    )

    print(
        "4.",
        os.path.join(
            os.path.expanduser("~"),
            "Documents"
        )
    )

    print()
    print(
        "No suitable GeoJSON containing "
        "properties.cellId was found."
    )

    print()
    print(
        "If your file is somewhere else, "
        "move/copy it into:"
    )

    print(PROJECT_DIR)

    print()
    print(
        "Expected filename:"
    )

    print(
        "milano-grid.geojson"
    )

    spark.stop()
    raise SystemExit(1)


GEOJSON_FILE = geojson_candidates[0]


print()
print("=" * 75)
print("GEOJSON FOUND SUCCESSFULLY")
print("=" * 75)

print()
print(
    "Using GeoJSON:"
)

print(
    GEOJSON_FILE
)


# ============================================================
# STEP 1
# INSPECT GEOJSON
# ============================================================

print()
print("=" * 75)
print("STEP 1 - INSPECTING GEOJSON STRUCTURE")
print("=" * 75)


try:

    with open(
        GEOJSON_FILE,
        "r",
        encoding="utf-8"
    ) as f:

        geojson_data = json.load(f)

except Exception as e:

    print()
    print("ERROR reading GeoJSON:")
    print(str(e))

    spark.stop()
    raise SystemExit(1)


top_level_type = geojson_data.get(
    "type"
)

features = geojson_data.get(
    "features",
    []
)


print()
print(
    "Top-level type:",
    top_level_type
)

print(
    "Number of features:",
    len(features)
)


if top_level_type != "FeatureCollection":

    print()
    print(
        "ERROR: Expected FeatureCollection."
    )

    spark.stop()
    raise SystemExit(1)


if len(features) == 0:

    print()
    print(
        "ERROR: GeoJSON has no features."
    )

    spark.stop()
    raise SystemExit(1)


# Inspect first feature
first_feature = features[0]

first_properties = first_feature.get(
    "properties",
    {}
)

first_geometry = first_feature.get(
    "geometry",
    {}
)


print()
print("First feature type:")
print(
    first_feature.get("type")
)

print()
print("First feature properties:")
print(first_properties)

print()
print("Grid identifier:")
print(
    first_properties.get("cellId")
)

print()
print("Geometry type:")
print(
    first_geometry.get("type")
)


# ============================================================
# VALIDATE GEOJSON
# ============================================================

print()
print("=" * 75)
print("VALIDATING GEOJSON")
print("=" * 75)


valid_features = []
cell_ids = []
geometry_types = set()

missing_cell_ids = 0
missing_geometries = 0


for feature in features:

    properties = feature.get(
        "properties",
        {}
    )

    geometry = feature.get(
        "geometry"
    )

    cell_id = properties.get(
        "cellId"
    )

    if cell_id is None:

        missing_cell_ids += 1

        continue

    if geometry is None:

        missing_geometries += 1

        continue

    cell_ids.append(
        str(cell_id)
    )

    geometry_type = geometry.get(
        "type"
    )

    if geometry_type:

        geometry_types.add(
            geometry_type
        )

    valid_features.append(
        (
            str(cell_id),
            json.dumps(
                geometry
            )
        )
    )


print()
print(
    "Total GeoJSON features:",
    len(features)
)

print(
    "Valid features:",
    len(valid_features)
)

print(
    "Missing cellId:",
    missing_cell_ids
)

print(
    "Missing geometry:",
    missing_geometries
)

print(
    "Geometry types:",
    geometry_types
)


# ============================================================
# CHECK DUPLICATE CELL IDs
# ============================================================

unique_cell_ids = len(
    set(cell_ids)
)

duplicate_cell_ids = (
    len(cell_ids)
    - unique_cell_ids
)


print()
print(
    "Unique cellIds:",
    unique_cell_ids
)

print(
    "Duplicate cellIds:",
    duplicate_cell_ids
)


if duplicate_cell_ids > 0:

    print()
    print(
        "WARNING: Duplicate cellIds detected."
    )


# ============================================================
# STEP 2/3
# CREATE GRID LOOKUP
#
# properties.cellId -> grid_id
# geometry -> geometry
# ============================================================

print()
print("=" * 75)
print("STEP 2 - CREATING GRID LOOKUP")
print("=" * 75)


grid_lookup = spark.createDataFrame(
    valid_features,
    [
        "grid_id",
        "geometry"
    ]
)


grid_lookup = (
    grid_lookup
    .dropDuplicates(
        ["grid_id"]
    )
)


lookup_count = grid_lookup.count()


print()
print(
    "Grid lookup created."
)

print(
    "Grid lookup rows:",
    lookup_count
)

print()
print(
    "Grid lookup schema:"
)

grid_lookup.printSchema()


print()
print(
    "Grid lookup sample:"
)

grid_lookup.show(
    5,
    truncate=False
)


# ============================================================
# LOAD SP3 DATA
# ============================================================

print()
print("=" * 75)
print("LOADING SP3 ACTIVITY DATA")
print("=" * 75)


activity_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(SP3_FILE)
)


# Normalize column names
activity_df = activity_df.toDF(
    *[
        c.strip()
        .lower()
        .replace(
            " ",
            "_"
        )
        for c in activity_df.columns
    ]
)


print()
print(
    "Activity columns:"
)

print(
    activity_df.columns
)


# ============================================================
# NORMALIZE INTERNET COLUMN
# ============================================================

if (
    "internet_activity"
    not in activity_df.columns
):

    if "internet" in activity_df.columns:

        activity_df = (
            activity_df
            .withColumnRenamed(
                "internet",
                "internet_activity"
            )
        )

    else:

        print()
        print(
            "ERROR: internet/internet_activity "
            "column not found."
        )

        spark.stop()
        raise SystemExit(1)


# ============================================================
# REQUIRED COLUMNS
# ============================================================

required_columns = [
    "timestamp",
    "grid_id",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_activity"
]


missing_columns = [
    c
    for c in required_columns
    if c not in activity_df.columns
]


if missing_columns:

    print()
    print(
        "ERROR: Missing SP3 columns:"
    )

    for c in missing_columns:
        print(
            "   ",
            c
        )

    spark.stop()
    raise SystemExit(1)


# ============================================================
# DATA TYPES
# ============================================================

activity_df = (
    activity_df
    .withColumn(
        "grid_id",
        col("grid_id").cast("string")
    )
    .withColumn(
        "timestamp",
        col("timestamp").cast("timestamp")
    )
)


# ============================================================
# QUESTION 4
# SIZE COMPARISON
# ============================================================

print()
print("=" * 75)
print("STEP 3 - SIZE COMPARISON")
print("=" * 75)


activity_count = activity_df.count()


distinct_activity_grids = (
    activity_df
    .select("grid_id")
    .where(
        col("grid_id").isNotNull()
    )
    .distinct()
    .count()
)


print()
print(
    "Activity rows:",
    activity_count
)

print(
    "Distinct activity grids:",
    distinct_activity_grids
)

print(
    "GeoJSON grid lookup rows:",
    lookup_count
)


# ============================================================
# QUESTION 5
# STANDARD LEFT JOIN
# ============================================================

print()
print("=" * 75)
print("STEP 4 - STANDARD LEFT JOIN")
print("=" * 75)


standard_join = (
    activity_df
    .join(
        grid_lookup,
        on="grid_id",
        how="left"
    )
)


standard_count = standard_join.count()


print()
print(
    "Rows before join:",
    activity_count
)

print(
    "Rows after standard join:",
    standard_count
)


if standard_count == activity_count:

    print()
    print(
        "PASS: Standard left join preserved row count."
    )

else:

    print()
    print(
        "WARNING: Standard join changed row count."
    )


# ============================================================
# QUESTION 8
# STANDARD EXECUTION PLAN
# ============================================================

print()
print("=" * 75)
print("STEP 5 - STANDARD JOIN EXECUTION PLAN")
print("=" * 75)


standard_join.explain(
    mode="formatted"
)


# ============================================================
# QUESTION 8
# BROADCAST JOIN
# ============================================================

print()
print("=" * 75)
print("STEP 6 - BROADCAST JOIN EXECUTION PLAN")
print("=" * 75)


broadcast_join = (
    activity_df
    .join(
        broadcast(
            grid_lookup
        ),
        on="grid_id",
        how="left"
    )
)


broadcast_join.explain(
    mode="formatted"
)


print()
print(
    "Broadcast reason:"
)

print(
    "The GeoJSON lookup is small compared "
    "with the activity DataFrame."
)

print(
    "Broadcasting avoids a large shuffle "
    "of the activity dataset."
)


# ============================================================
# USE BROADCAST JOIN
# ============================================================

enriched_network_activity = (
    broadcast_join
)


# ============================================================
# QUESTION 6
# JOIN VALIDATION
# ============================================================

print()
print("=" * 75)
print("STEP 7 - NUMERICAL JOIN VALIDATION")
print("=" * 75)


distinct_after = (
    enriched_network_activity
    .select("grid_id")
    .where(
        col("grid_id").isNotNull()
    )
    .distinct()
    .count()
)


missing_geometry = (
    enriched_network_activity
    .filter(
        col("geometry").isNull()
    )
    .select("grid_id")
    .distinct()
    .count()
)


matched_grids = (
    enriched_network_activity
    .filter(
        col("geometry").isNotNull()
    )
    .select("grid_id")
    .distinct()
    .count()
)


if distinct_activity_grids > 0:

    enrichment_percentage = (
        matched_grids
        / distinct_activity_grids
        * 100
    )

else:

    enrichment_percentage = 0.0


print()
print(
    "Distinct activity grids BEFORE join:",
    distinct_activity_grids
)

print(
    "Distinct activity grids AFTER join:",
    distinct_after
)

print(
    "Grids with missing geometry:",
    missing_geometry
)

print(
    "Successfully enriched grids:",
    matched_grids
)

print(
    "Enrichment percentage:",
    round(
        enrichment_percentage,
        2
    ),
    "%"
)


# ============================================================
# UNMATCHED GRID IDS
# ============================================================

print()
print("=" * 75)
print("CHECKING UNMATCHED GRID IDs")
print("=" * 75)


activity_ids = (
    activity_df
    .select("grid_id")
    .where(
        col("grid_id").isNotNull()
    )
    .distinct()
)


lookup_ids = (
    grid_lookup
    .select("grid_id")
    .distinct()
)


unmatched = (
    activity_ids
    .join(
        lookup_ids,
        on="grid_id",
        how="left_anti"
    )
)


unmatched_count = unmatched.count()


print()
print(
    "Activity grids without GeoJSON match:",
    unmatched_count
)


if unmatched_count == 0:

    print(
        "PASS: All activity grids have GeoJSON matches."
    )

else:

    print(
        "WARNING: Some activity grids are unmatched."
    )

    unmatched.show(
        20,
        truncate=False
    )


# ============================================================
# QUESTION 7
# GEOGRAPHIC VALIDATION
# ============================================================

print()
print("=" * 75)
print("STEP 8 - GEOGRAPHIC VALIDATION")
print("=" * 75)


print()
print(
    "Geometry types found:"
)

for geometry_type in sorted(
    geometry_types
):

    print(
        "   ",
        geometry_type
    )


# ------------------------------------------------------------
# Validate sampled geometries
# ------------------------------------------------------------

sample = (
    grid_lookup
    .limit(20)
    .collect()
)


valid_sample = 0
invalid_sample = 0


for row in sample:

    try:

        geometry_object = json.loads(
            row["geometry"]
        )

        geometry_type = (
            geometry_object.get(
                "type"
            )
        )

        coordinates = (
            geometry_object.get(
                "coordinates"
            )
        )

        if (
            geometry_type
            and coordinates
        ):

            valid_sample += 1

        else:

            invalid_sample += 1

    except Exception:

        invalid_sample += 1


print()
print(
    "Sampled geometries:",
    len(sample)
)

print(
    "Valid sampled geometries:",
    valid_sample
)

print(
    "Invalid sampled geometries:",
    invalid_sample
)


if invalid_sample == 0:

    print()
    print(
        "PASS: Sampled geometries are structurally valid."
    )

else:

    print()
    print(
        "WARNING: Some sampled geometries are invalid."
    )


# ------------------------------------------------------------
# Geographic bounding box
# ------------------------------------------------------------

def extract_coordinates(obj):

    coordinates = []

    if isinstance(
        obj,
        list
    ):

        if (
            len(obj) >= 2
            and isinstance(
                obj[0],
                (int, float)
            )
            and isinstance(
                obj[1],
                (int, float)
            )
        ):

            coordinates.append(
                (
                    float(obj[0]),
                    float(obj[1])
                )
            )

        else:

            for item in obj:

                coordinates.extend(
                    extract_coordinates(
                        item
                    )
                )

    return coordinates


all_coordinates = []


for feature in features:

    geometry = feature.get(
        "geometry"
    )

    if geometry:

        coords = geometry.get(
            "coordinates"
        )

        all_coordinates.extend(
            extract_coordinates(
                coords
            )
        )


if all_coordinates:

    longitudes = [
        x[0]
        for x in all_coordinates
    ]

    latitudes = [
        x[1]
        for x in all_coordinates
    ]

    min_lon = min(longitudes)
    max_lon = max(longitudes)

    min_lat = min(latitudes)
    max_lat = max(latitudes)


    print()
    print(
        "Geographic bounding box:"
    )

    print(
        "Longitude:",
        min_lon,
        "to",
        max_lon
    )

    print(
        "Latitude:",
        min_lat,
        "to",
        max_lat
    )


    # Broad Milan sanity check
    if (
        8.0 <= min_lon <= 11.0
        and 8.0 <= max_lon <= 11.0
        and 44.0 <= min_lat <= 46.5
        and 44.0 <= max_lat <= 46.5
    ):

        print()
        print(
            "PASS: Coordinates are geographically "
            "consistent with the Milan region."
        )

    else:

        print()
        print(
            "WARNING: Coordinate extent does not "
            "look like the expected Milan region."
        )


# ============================================================
# QUESTION 9
# FINAL ENRICHED DATASET
# ============================================================

print()
print("=" * 75)
print("STEP 9 - CREATING ENRICHED DATASET")
print("=" * 75)


enriched_network_activity = (
    enriched_network_activity
    .select(
        "timestamp",
        "grid_id",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
        "total_activity",
        "geometry"
    )
)


print()
print(
    "Final enriched schema:"
)

enriched_network_activity.printSchema()


print()
print(
    "Sample enriched data:"
)

enriched_network_activity.show(
    10,
    truncate=False
)


# ============================================================
# FINAL VALIDATION
# ============================================================

final_rows = (
    enriched_network_activity.count()
)


final_missing_geometry = (
    enriched_network_activity
    .filter(
        col("geometry").isNull()
    )
    .select("grid_id")
    .distinct()
    .count()
)


print()
print("=" * 75)
print("FINAL ENRICHMENT VALIDATION")
print("=" * 75)


print()
print(
    "Final enriched rows:",
    final_rows
)

print(
    "Missing geometry grids:",
    final_missing_geometry
)


if final_rows == activity_count:

    print()
    print(
        "PASS: Final enriched dataset "
        "preserved all activity rows."
    )

else:

    print()
    print(
        "WARNING: Final row count differs "
        "from original activity data."
    )


# ============================================================
# QUESTION 10
# TOP HIGH-ACTIVITY GRIDS
# ============================================================

print()
print("=" * 75)
print("STEP 10 - TOP HIGH-ACTIVITY GRIDS")
print("=" * 75)


top10_high_activity_grids = (
    enriched_network_activity
    .groupBy(
        "grid_id",
        "geometry"
    )
    .agg(
        spark_sum(
            "total_activity"
        ).alias(
            "total_activity"
        )
    )
    .orderBy(
        desc("total_activity")
    )
    .limit(10)
)


print()
print(
    "Top 10 high-activity grids:"
)

top10_high_activity_grids.show(
    10,
    truncate=False
)


# ============================================================
# SAVE SP4 OUTPUTS
# ============================================================

print()
print("=" * 75)
print("SAVING SP4 OUTPUTS")
print("=" * 75)


import csv


# ============================================================
# HELPER FUNCTION
# WRITE SPARK DATAFRAME DIRECTLY USING PYTHON
#
# This intentionally avoids Spark .write.csv()
# so Hadoop/winutils is NOT required for output.
# ============================================================

def write_dataframe_to_csv(
    dataframe,
    output_directory,
    filename
):

    os.makedirs(
        output_directory,
        exist_ok=True
    )

    output_file = os.path.join(
        output_directory,
        filename
    )

    print()
    print(
        "Writing:",
        output_file
    )

    rows = dataframe.toLocalIterator()

    first_row = True
    row_count = 0

    with open(
        output_file,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = None

        for row in rows:

            row_dict = row.asDict(
                recursive=True
            )

            if first_row:

                writer = csv.DictWriter(
                    f,
                    fieldnames=list(
                        row_dict.keys()
                    )
                )

                writer.writeheader()

                first_row = False

            writer.writerow(
                row_dict
            )

            row_count += 1

            # Progress indicator
            if row_count % 100000 == 0:

                print(
                    f"   {row_count:,} rows written..."
                )

    print()
    print(
        "Completed:",
        filename
    )

    print(
        "Rows written:",
        f"{row_count:,}"
    )

    return output_file


# ============================================================
# 1. SAVE ENRICHED NETWORK ACTIVITY
# ============================================================

enriched_output_dir = os.path.join(
    OUTPUT_DIR,
    "enriched_network_activity"
)


enriched_output_file = write_dataframe_to_csv(
    enriched_network_activity,
    enriched_output_dir,
    "enriched_network_activity.csv"
)


# ============================================================
# 2. SAVE TOP 10 HIGH-ACTIVITY GRIDS
# ============================================================

top10_output_dir = os.path.join(
    OUTPUT_DIR,
    "top10_high_activity_grids"
)


top10_output_file = write_dataframe_to_csv(
    top10_high_activity_grids,
    top10_output_dir,
    "top10_high_activity_grids.csv"
)


# ============================================================
# 3. SAVE MILANO GRID LOOKUP
# ============================================================

lookup_output_dir = os.path.join(
    OUTPUT_DIR,
    "milano_grid_lookup"
)


lookup_output_file = write_dataframe_to_csv(
    grid_lookup,
    lookup_output_dir,
    "milano_grid_lookup.csv"
)


# ============================================================
# FINAL SUCCESS VALIDATION
# ============================================================

print()
print("=" * 75)
print("SP4 COMPLETED SUCCESSFULLY")
print("=" * 75)


print()
print("Enriched network activity:")
print(
    enriched_output_file
)


print()
print("Top 10 high-activity grids:")
print(
    top10_output_file
)


print()
print("Milan grid lookup:")
print(
    lookup_output_file
)


print()
print(
    "Enrichment percentage:",
    round(
        enrichment_percentage,
        2
    ),
    "%"
)


print()
print(
    "Missing geometry grids:",
    final_missing_geometry
)


print()
print("=" * 75)
print("ALL SP4 OUTPUTS CREATED SUCCESSFULLY")
print("=" * 75)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()