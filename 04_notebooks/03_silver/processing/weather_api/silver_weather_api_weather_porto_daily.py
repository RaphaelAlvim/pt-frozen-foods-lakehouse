# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: weather_porto_daily
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "weather_api"
DATASET = "weather_porto_daily"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

EXPECTED_CITY = "Porto"
VALID_CHOVEU_VALUES = [0, 1]


# ========================================
# 1. CONTEXT SETUP
# ========================================

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print("Context configured successfully")
print(f"Catalog: {spark.catalog.currentCatalog()}")
print(f"Schema: {spark.catalog.currentDatabase()}")


# ========================================
# 2. CONFIGURATION SUMMARY
# ========================================

print(" ")
print("========== CONFIGURATION SUMMARY ==========")
print(f"Source table    : {SOURCE_TABLE}")
print(f"Target table    : {TARGET_TABLE}")
print(f"Source path     : {SOURCE_PATH}")
print(f"Target path     : {TARGET_PATH}")
print(f"Expected city   : {EXPECTED_CITY}")
print("===========================================")


# ========================================
# 3. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {SOURCE_TABLE}")

print("[INFO] Checking source path access...")
source_items = dbutils.fs.ls(SOURCE_PATH)

print("[INFO] Checking target container access...")
target_items = dbutils.fs.ls(f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

if len(source_items) == 0:
    raise ValueError(f"[ERROR] No files found in source path: {SOURCE_PATH}")

print(" ")
print("Pre-checks completed successfully")
print(f"Source path accessible     : yes ({len(source_items)} items found)")
print(f"Target container access    : yes ({len(target_items)} items found)")


# ========================================
# 4. READ SOURCE DATA
# ========================================

df_source = spark.table(SOURCE_TABLE)
source_row_count = df_source.count()

print(" ")
print("Source data loaded successfully")
print(f"Source table     : {SOURCE_TABLE}")
print(f"Source row count : {source_row_count}")


# ========================================
# 5. CLEANING AND STANDARDIZATION
# ========================================

print(" ")
print("[INFO] Starting cleaning and standardization...")

df_silver = df_source.select(
    "data",
    "cidade",
    "temperatura_media",
    "temperatura_min",
    "temperatura_max",
    "choveu",
    "precipitacao_mm",
    "humidade_media",
    "vento_kmh",
    "fonte_api",
    "load_date",
    "ingestion_timestamp",
    "source_file"
)

print("[INFO] Selected relevant columns for Silver layer")

string_columns = [
    field.name
    for field in df_silver.schema.fields
    if isinstance(field.dataType, StringType)
]

for col_name in string_columns:
    df_silver = df_silver.withColumn(
        col_name,
        F.when(F.trim(F.col(col_name)) == "", None)
         .otherwise(F.trim(F.col(col_name)))
    )

print("[INFO] Applied base string standardization (trim + empty -> null)")

before_dedup_count = df_silver.count()
df_silver = df_silver.dropDuplicates()
after_dedup_count = df_silver.count()

print(f"[INFO] Exact-row deduplication applied: {before_dedup_count} -> {after_dedup_count}")


# ========================================
# 6. DATA QUALITY VALIDATION
# ========================================

silver_row_count = df_silver.count()

print(" ")
print("[INFO] Data quality validation completed")
print(f"[INFO] Silver row count : {silver_row_count}")

duplicate_data_count = (
    df_silver.groupBy("data")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"[INFO] Duplicate date groups found after transformation: {duplicate_data_count}")


# ========================================
# 7. CRITICAL BUSINESS VALIDATION
# ========================================

print("[INFO] Validating business rules...")

null_data = df_silver.filter(F.col("data").isNull()).count()
null_cidade = df_silver.filter(F.col("cidade").isNull()).count()
null_fonte_api = df_silver.filter(F.col("fonte_api").isNull()).count()

invalid_choveu_count = df_silver.filter(
    ~F.col("choveu").isin(VALID_CHOVEU_VALUES)
).count()

invalid_humidade_count = df_silver.filter(
    (F.col("humidade_media") < 0) | (F.col("humidade_media") > 100)
).count()

invalid_precipitacao_count = df_silver.filter(
    F.col("precipitacao_mm") < 0
).count()

invalid_vento_count = df_silver.filter(
    F.col("vento_kmh") < 0
).count()

invalid_temperatura_interval_count = df_silver.filter(
    (F.col("temperatura_min") > F.col("temperatura_media")) |
    (F.col("temperatura_media") > F.col("temperatura_max")) |
    (F.col("temperatura_min") > F.col("temperatura_max"))
).count()

invalid_cidade_count = df_silver.filter(
    F.col("cidade") != EXPECTED_CITY
).count()

if null_data > 0:
    raise ValueError(f"[ERROR] data contains {null_data} null values")

if null_cidade > 0:
    raise ValueError(f"[ERROR] cidade contains {null_cidade} null values")

if null_fonte_api > 0:
    raise ValueError(f"[ERROR] fonte_api contains {null_fonte_api} null values")

if duplicate_data_count > 0:
    raise ValueError(f"[ERROR] data contains {duplicate_data_count} duplicate groups after transformation")

if invalid_choveu_count > 0:
    raise ValueError(f"[ERROR] choveu contains {invalid_choveu_count} invalid values")

if invalid_humidade_count > 0:
    raise ValueError(f"[ERROR] humidade_media contains {invalid_humidade_count} invalid values")

if invalid_precipitacao_count > 0:
    raise ValueError(f"[ERROR] precipitacao_mm contains {invalid_precipitacao_count} negative values")

if invalid_vento_count > 0:
    raise ValueError(f"[ERROR] vento_kmh contains {invalid_vento_count} negative values")

if invalid_temperatura_interval_count > 0:
    raise ValueError(f"[ERROR] temperature interval validation failed in {invalid_temperatura_interval_count} rows")

if invalid_cidade_count > 0:
    raise ValueError(f"[ERROR] cidade contains {invalid_cidade_count} unexpected values different from {EXPECTED_CITY}")

if silver_row_count == 0:
    raise ValueError("[ERROR] Silver dataset is empty after transformations")

print("[INFO] data validation passed (no nulls)")
print("[INFO] cidade validation passed (no nulls)")
print("[INFO] fonte_api validation passed (no nulls)")
print("[INFO] choveu domain validation passed")
print("[INFO] humidade_media validation passed")
print("[INFO] precipitacao_mm validation passed")
print("[INFO] vento_kmh validation passed")
print("[INFO] temperature interval validation passed")
print("[INFO] city validation passed")
print("[INFO] Silver dataset is not empty")


# ========================================
# 8. WRITE TO DELTA
# ========================================

print(" ")
print(f"[INFO] Writing Silver dataset to target path: {TARGET_PATH}")

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(TARGET_PATH)
)

print("[INFO] Silver dataset written successfully")


# ========================================
# 9. REGISTER TABLE
# ========================================

print(f"[INFO] Registering target table: {TARGET_TABLE}")

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

spark.sql(f"""
CREATE TABLE {TARGET_TABLE}
USING DELTA
LOCATION '{TARGET_PATH}'
""")

print("[INFO] Target table registered successfully")


# ========================================
# 10. FINAL STATUS
# ========================================

print(" ")
print("===========================================")
print("SILVER PROCESS COMPLETED SUCCESSFULLY")
print(f"Dataset         : {DATASET}")
print(f"Source table    : {SOURCE_TABLE}")
print(f"Target table    : {TARGET_TABLE}")
print(f"Target path     : {TARGET_PATH}")
print(f"Source row count: {source_row_count}")
print(f"Target row count: {silver_row_count}")
print("===========================================")