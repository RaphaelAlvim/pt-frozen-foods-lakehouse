# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: reference_locations
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "reference"
DATASET = "reference_locations"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

VALID_REGION_VALUES = [
    "Norte",
    "Centro",
    "Lisboa",
    "Alentejo",
    "Algarve",
    "Açores",
    "Madeira"
]

CLUSTER_COLUMNS = [
    "regiao",
    "distrito",
    "localidade_id"
]

REQUIRED_COLUMNS = [
    "localidade_id",
    "cidade",
    "distrito",
    "regiao",
    "load_date",
    "ingestion_timestamp",
    "source_file"
]

print("=" * 80)
print("STARTING SILVER PROCESSING: reference_locations")
print("=" * 80)

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print("[INFO] Context setup completed successfully.")

# ========================================
# 1. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {SOURCE_TABLE}")

print("[INFO] Checking source path access...")
source_items = dbutils.fs.ls(SOURCE_PATH)

if len(source_items) == 0:
    raise ValueError(f"No files found in source path: {SOURCE_PATH}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")

# ========================================
# 2. SOURCE VALIDATION
# ========================================

print("[INFO] Validating source dataset...")

df_source = spark.table(SOURCE_TABLE)

missing_columns = [c for c in REQUIRED_COLUMNS if c not in df_source.columns]

if missing_columns:
    raise ValueError(f"Missing required columns in source dataset: {missing_columns}")

source_validation = (
    df_source
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("localidade_id").isNull(), 1).otherwise(0)).alias("null_localidade_id"),
        F.sum(F.when(F.col("cidade").isNull(), 1).otherwise(0)).alias("null_cidade"),
        F.sum(F.when(F.col("distrito").isNull(), 1).otherwise(0)).alias("null_distrito"),
        F.sum(F.when(F.col("regiao").isNull(), 1).otherwise(0)).alias("null_regiao")
    )
    .collect()[0]
)

print(f"Source row count:        {source_validation['row_count']:,}")
print(f"Null localidade_id:      {source_validation['null_localidade_id']:,}")
print(f"Null cidade:             {source_validation['null_cidade']:,}")
print(f"Null distrito:           {source_validation['null_distrito']:,}")
print(f"Null regiao:             {source_validation['null_regiao']:,}")

if source_validation["row_count"] == 0:
    raise ValueError("Source dataset is empty.")

source_null_failures = {
    "localidade_id": source_validation["null_localidade_id"],
    "cidade": source_validation["null_cidade"],
    "distrito": source_validation["null_distrito"],
    "regiao": source_validation["null_regiao"]
}

source_null_failures = {column: count for column, count in source_null_failures.items() if count > 0}

if source_null_failures:
    raise ValueError(f"Null values detected in source critical columns: {source_null_failures}")

print("[INFO] Source validation completed successfully.")

# ========================================
# 3. CREATE SILVER TABLE
# ========================================

print("[INFO] Creating Silver table using CTAS...")

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET_TABLE}
USING DELTA
LOCATION '{TARGET_PATH}'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
CLUSTER BY ({", ".join(CLUSTER_COLUMNS)})
AS
SELECT DISTINCT
    NULLIF(TRIM(localidade_id), '') AS localidade_id,
    NULLIF(TRIM(cidade), '') AS cidade,
    NULLIF(TRIM(distrito), '') AS distrito,
    NULLIF(TRIM(regiao), '') AS regiao,

    load_date,
    ingestion_timestamp,
    source_file

FROM {SOURCE_TABLE}
""")

print("[INFO] Silver table created successfully.")

# ========================================
# 4. OPTIMIZATION
# ========================================

print("[INFO] Running OPTIMIZE...")

spark.sql(f"OPTIMIZE {TARGET_TABLE}")

print("[INFO] Optimization completed.")

# ========================================
# 5. FINAL VALIDATIONS
# ========================================

print("=" * 80)
print("FINAL VALIDATIONS")
print("=" * 80)

df_target = spark.table(TARGET_TABLE)

final = (
    df_target
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("localidade_id").alias("distinct_localidade_ids"),
        F.sum(F.when(F.col("localidade_id").isNull(), 1).otherwise(0)).alias("null_localidade_id"),
        F.sum(F.when(F.col("cidade").isNull(), 1).otherwise(0)).alias("null_cidade"),
        F.sum(F.when(F.col("distrito").isNull(), 1).otherwise(0)).alias("null_distrito"),
        F.sum(F.when(F.col("regiao").isNull(), 1).otherwise(0)).alias("null_regiao"),
        F.sum(F.when(~F.col("regiao").isin(VALID_REGION_VALUES), 1).otherwise(0)).alias("invalid_regiao")
    )
    .collect()[0]
)

duplicate_localidade_id_records = final["row_count"] - final["distinct_localidade_ids"]

print(f"Rows:                             {final['row_count']:,}")
print(f"Duplicate localidade_id records:  {duplicate_localidade_id_records:,}")
print(f"Null localidade_id:               {final['null_localidade_id']}")
print(f"Null cidade:                      {final['null_cidade']}")
print(f"Null distrito:                    {final['null_distrito']}")
print(f"Null regiao:                      {final['null_regiao']}")
print(f"Invalid regiao:                   {final['invalid_regiao']}")

if final["row_count"] == 0:
    raise ValueError("Silver dataset is empty.")

critical_nulls = {
    "localidade_id": final["null_localidade_id"],
    "cidade": final["null_cidade"],
    "distrito": final["null_distrito"],
    "regiao": final["null_regiao"]
}

null_failures = {column: count for column, count in critical_nulls.items() if count > 0}

if null_failures:
    raise ValueError(f"Null values detected in critical columns: {null_failures}")

if duplicate_localidade_id_records > 0:
    raise ValueError(f"Duplicate localidade_id detected after transformation: {duplicate_localidade_id_records}")

if final["invalid_regiao"] > 0:
    raise ValueError(f"Invalid regiao values detected: {final['invalid_regiao']}")

print("[INFO] Final validations completed.")

# ========================================
# 6. FINAL STATUS
# ========================================

detail = spark.sql(f"DESCRIBE DETAIL {TARGET_TABLE}").collect()[0].asDict()

print("=" * 80)
print("FINAL TABLE DETAIL")
print("=" * 80)
print(f"Files: {detail.get('numFiles')}")
print(f"Size:  {detail.get('sizeInBytes')}")

print("=" * 80)
print("COMPLETED")
print("=" * 80)