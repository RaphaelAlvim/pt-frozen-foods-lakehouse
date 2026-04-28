# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: crm_clients
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "crm"
DATASET = "crm_clients"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_COLUMNS = [
    "cliente_id",
    "tipo_cliente"
]

REQUIRED_COLUMNS = [
    "cliente_id",
    "nome_cliente",
    "tipo_cliente",
    "nif",
    "data_registo",
    "cidade",
    "distrito",
    "canal_captacao",
    "score_atividade",
    "obs_comercial",
    "load_date",
    "ingestion_timestamp",
    "source_file"
]

print("=" * 80)
print("STARTING SILVER PROCESSING: crm_clients")
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
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id")
    )
    .collect()[0]
)

print(f"Source row count:      {source_validation['row_count']:,}")
print(f"Null cliente_id:       {source_validation['null_cliente_id']:,}")

if source_validation["row_count"] == 0:
    raise ValueError("Source dataset is empty.")

if source_validation["null_cliente_id"] > 0:
    raise ValueError("Null cliente_id detected in source dataset.")

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
    NULLIF(TRIM(cliente_id), '') AS cliente_id,
    NULLIF(TRIM(nome_cliente), '') AS nome_cliente,
    NULLIF(TRIM(tipo_cliente), '') AS tipo_cliente,
    CAST(NULLIF(TRIM(nif), '') AS STRING) AS nif,

    COALESCE(
        TO_DATE(data_registo, 'yyyy-MM-dd'),
        TO_DATE(data_registo, 'yyyy/MM/dd'),
        TO_DATE(data_registo, 'dd/MM/yyyy')
    ) AS data_registo,

    NULLIF(TRIM(cidade), '') AS cidade,
    NULLIF(TRIM(distrito), '') AS distrito,
    NULLIF(TRIM(canal_captacao), '') AS canal_captacao,
    score_atividade,
    NULLIF(TRIM(obs_comercial), '') AS obs_comercial,
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
        F.countDistinct("cliente_id").alias("distinct_cliente_ids"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id"),
        F.sum(F.when(F.col("nome_cliente").isNull(), 1).otherwise(0)).alias("null_nome_cliente"),
        F.sum(F.when(F.col("tipo_cliente").isNull(), 1).otherwise(0)).alias("null_tipo_cliente"),
        F.sum(F.when(F.col("data_registo").isNull(), 1).otherwise(0)).alias("null_data_registo")
    )
    .collect()[0]
)

duplicate_cliente_id_records = final["row_count"] - final["distinct_cliente_ids"]

print(f"Rows:                           {final['row_count']:,}")
print(f"Duplicate cliente_id records:   {duplicate_cliente_id_records:,}")
print("[INFO] Duplicate cliente_id records are reported for monitoring only.")
print(f"Null cliente_id:                {final['null_cliente_id']}")
print(f"Null nome_cliente:              {final['null_nome_cliente']}")
print(f"Null tipo_cliente:              {final['null_tipo_cliente']}")
print(f"Null data_registo:              {final['null_data_registo']}")

if final["row_count"] == 0:
    raise ValueError("Silver dataset is empty.")

if final["null_cliente_id"] > 0:
    raise ValueError("Null cliente_id detected.")

critical_nulls = {
    "nome_cliente": final["null_nome_cliente"],
    "tipo_cliente": final["null_tipo_cliente"],
    "data_registo": final["null_data_registo"]
}

null_failures = {column: count for column, count in critical_nulls.items() if count > 0}

if null_failures:
    raise ValueError(f"Null values detected in critical columns: {null_failures}")

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