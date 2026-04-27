# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: reference_sales_channels
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "reference"
DATASET = "reference_sales_channels"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_COLUMNS = [
    "canal_id",
    "nome_canal"
]

REQUIRED_COLUMNS = [
    "canal_id",
    "nome_canal",
    "descricao",
    "load_date",
    "ingestion_timestamp",
    "source_file"
]

print("=" * 80)
print("STARTING SILVER PROCESSING: reference_sales_channels")
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
        F.sum(F.when(F.col("canal_id").isNull(), 1).otherwise(0)).alias("null_canal_id"),
        F.sum(F.when(F.col("nome_canal").isNull(), 1).otherwise(0)).alias("null_nome_canal"),
        F.sum(F.when(F.col("descricao").isNull(), 1).otherwise(0)).alias("null_descricao")
    )
    .collect()[0]
)

print(f"Source row count:      {source_validation['row_count']:,}")
print(f"Null canal_id:         {source_validation['null_canal_id']:,}")
print(f"Null nome_canal:       {source_validation['null_nome_canal']:,}")
print(f"Null descricao:        {source_validation['null_descricao']:,}")

if source_validation["row_count"] == 0:
    raise ValueError("Source dataset is empty.")

source_null_failures = {
    "canal_id": source_validation["null_canal_id"],
    "nome_canal": source_validation["null_nome_canal"],
    "descricao": source_validation["null_descricao"]
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
    NULLIF(TRIM(canal_id), '') AS canal_id,
    NULLIF(TRIM(nome_canal), '') AS nome_canal,
    NULLIF(TRIM(descricao), '') AS descricao,

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
        F.countDistinct("canal_id").alias("distinct_canal_ids"),
        F.sum(F.when(F.col("canal_id").isNull(), 1).otherwise(0)).alias("null_canal_id"),
        F.sum(F.when(F.col("nome_canal").isNull(), 1).otherwise(0)).alias("null_nome_canal"),
        F.sum(F.when(F.col("descricao").isNull(), 1).otherwise(0)).alias("null_descricao")
    )
    .collect()[0]
)

duplicate_canal_id_records = final["row_count"] - final["distinct_canal_ids"]

print(f"Rows:                       {final['row_count']:,}")
print(f"Duplicate canal_id records: {duplicate_canal_id_records:,}")
print(f"Null canal_id:              {final['null_canal_id']}")
print(f"Null nome_canal:            {final['null_nome_canal']}")
print(f"Null descricao:             {final['null_descricao']}")

if final["row_count"] == 0:
    raise ValueError("Silver dataset is empty.")

critical_nulls = {
    "canal_id": final["null_canal_id"],
    "nome_canal": final["null_nome_canal"],
    "descricao": final["null_descricao"]
}

null_failures = {column: count for column, count in critical_nulls.items() if count > 0}

if null_failures:
    raise ValueError(f"Null values detected in critical columns: {null_failures}")

if duplicate_canal_id_records > 0:
    raise ValueError(f"Duplicate canal_id detected after transformation: {duplicate_canal_id_records}")

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