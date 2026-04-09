# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: reference_calendar
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "reference"
DATASET = "reference_calendar"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"


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
    "ano",
    "mes",
    "dia",
    "trimestre",
    "nome_mes",
    "dia_semana",
    "nome_dia_semana",
    "is_fim_de_semana",
    "is_inicio_mes",
    "is_fim_mes",
    "load_date",
    "ingestion_timestamp",
    "source_file"
)

print("[INFO] Selected relevant columns for Silver layer")

# Standardize strings
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

# Dedup (safety — should not remove anything)
before_dedup_count = df_silver.count()
df_silver = df_silver.dropDuplicates()
after_dedup_count = df_silver.count()

print(f"[INFO] Deduplication applied: {before_dedup_count} -> {after_dedup_count}")


# ========================================
# 6. DATA QUALITY VALIDATION
# ========================================

silver_row_count = df_silver.count()

print(" ")
print("[INFO] Data quality validation completed")
print(f"[INFO] Silver row count : {silver_row_count}")

duplicate_date_count = (
    df_silver.groupBy("data")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"[INFO] Duplicate date values: {duplicate_date_count}")


# ========================================
# 7. CRITICAL BUSINESS VALIDATION
# ========================================

print("[INFO] Validating business rules...")

# Null checks
if df_silver.filter(F.col("data").isNull()).count() > 0:
    raise ValueError("[ERROR] data contains null values")

# Date consistency checks
invalid_month = df_silver.filter((F.col("mes") < 1) | (F.col("mes") > 12)).count()
invalid_day = df_silver.filter((F.col("dia") < 1) | (F.col("dia") > 31)).count()
invalid_quarter = df_silver.filter((F.col("trimestre") < 1) | (F.col("trimestre") > 4)).count()

if invalid_month > 0:
    raise ValueError(f"[ERROR] Invalid month values detected: {invalid_month}")

if invalid_day > 0:
    raise ValueError(f"[ERROR] Invalid day values detected: {invalid_day}")

if invalid_quarter > 0:
    raise ValueError(f"[ERROR] Invalid quarter values detected: {invalid_quarter}")

# Cross-check: ano/mes/dia vs data
date_mismatch = df_silver.filter(
    (F.year("data") != F.col("ano")) |
    (F.month("data") != F.col("mes")) |
    (F.dayofmonth("data") != F.col("dia"))
).count()

if date_mismatch > 0:
    raise ValueError(f"[ERROR] Date decomposition mismatch detected: {date_mismatch}")

if silver_row_count == 0:
    raise ValueError("[ERROR] Silver dataset is empty")

print("[INFO] Business validations passed")


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