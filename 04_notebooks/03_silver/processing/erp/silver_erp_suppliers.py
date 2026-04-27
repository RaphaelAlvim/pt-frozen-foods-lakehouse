# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: erp_suppliers
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "erp"
DATASET = "erp_suppliers"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

VALID_STATUS_VALUES = ["Ativo", "Inativo"]

CLUSTER_COLUMNS = [
    "pais",
    "status_fornecedor",
    "fornecedor_id"
]

REQUIRED_COLUMNS = [
    "fornecedor_id",
    "nome_fornecedor",
    "pais",
    "status_fornecedor",
    "codigo_legacy",
    "ultima_sincronizacao",
    "load_date",
    "ingestion_timestamp",
    "source_file"
]

print("=" * 80)
print("STARTING SILVER PROCESSING: erp_suppliers")
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
        F.sum(F.when(F.col("fornecedor_id").isNull(), 1).otherwise(0)).alias("null_fornecedor_id"),
        F.sum(F.when(F.col("nome_fornecedor").isNull(), 1).otherwise(0)).alias("null_nome_fornecedor"),
        F.sum(F.when(F.col("pais").isNull(), 1).otherwise(0)).alias("null_pais")
    )
    .collect()[0]
)

print(f"Source row count:           {source_validation['row_count']:,}")
print(f"Null fornecedor_id:         {source_validation['null_fornecedor_id']:,}")
print(f"Null nome_fornecedor:       {source_validation['null_nome_fornecedor']:,}")
print(f"Null pais:                  {source_validation['null_pais']:,}")

if source_validation["row_count"] == 0:
    raise ValueError("Source dataset is empty.")

source_null_failures = {
    "fornecedor_id": source_validation["null_fornecedor_id"],
    "nome_fornecedor": source_validation["null_nome_fornecedor"],
    "pais": source_validation["null_pais"]
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
    NULLIF(TRIM(fornecedor_id), '') AS fornecedor_id,
    NULLIF(TRIM(nome_fornecedor), '') AS nome_fornecedor,
    NULLIF(TRIM(pais), '') AS pais,
    NULLIF(TRIM(status_fornecedor), '') AS status_fornecedor,
    NULLIF(TRIM(codigo_legacy), '') AS codigo_legacy,
    CAST(ultima_sincronizacao AS TIMESTAMP) AS ultima_sincronizacao,

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
        F.countDistinct("fornecedor_id").alias("distinct_fornecedor_ids"),
        F.sum(F.when(F.col("fornecedor_id").isNull(), 1).otherwise(0)).alias("null_fornecedor_id"),
        F.sum(F.when(F.col("nome_fornecedor").isNull(), 1).otherwise(0)).alias("null_nome_fornecedor"),
        F.sum(F.when(F.col("pais").isNull(), 1).otherwise(0)).alias("null_pais")
    )
    .collect()[0]
)

duplicate_fornecedor_id_records = final["row_count"] - final["distinct_fornecedor_ids"]

invalid_status_count = df_target.filter(
    ~F.col("status_fornecedor").isin(VALID_STATUS_VALUES)
).count()

future_sync_count = df_target.filter(
    F.col("ultima_sincronizacao") > F.current_timestamp()
).count()

print(f"Rows:                            {final['row_count']:,}")
print(f"Repeated fornecedor_id records:  {duplicate_fornecedor_id_records:,}")
print(f"Null fornecedor_id:              {final['null_fornecedor_id']}")
print(f"Null nome_fornecedor:            {final['null_nome_fornecedor']}")
print(f"Null pais:                       {final['null_pais']}")

if final["row_count"] == 0:
    raise ValueError("Silver dataset is empty.")

critical_nulls = {
    "fornecedor_id": final["null_fornecedor_id"],
    "nome_fornecedor": final["null_nome_fornecedor"],
    "pais": final["null_pais"]
}

null_failures = {column: count for column, count in critical_nulls.items() if count > 0}

if null_failures:
    raise ValueError(f"Null values detected in critical columns: {null_failures}")

if invalid_status_count > 0:
    raise ValueError(f"Invalid status_fornecedor values detected: {invalid_status_count}")

if future_sync_count > 0:
    raise ValueError(f"ultima_sincronizacao in the future detected: {future_sync_count}")

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