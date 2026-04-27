# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: erp_orders
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "erp"
DATASET = "erp_orders"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_COLUMNS = [
    "data_pedido",
    "cliente_id",
    "canal_id"
]

REQUIRED_COLUMNS = [
    "pedido_id",
    "data_pedido",
    "cliente_id",
    "canal_id",
    "vendedor_id",
    "cidade_entrega",
    "estado_pedido",
    "prazo_entrega_dias",
    "observacao_pedido",
    "sistema_origem",
    "usuario_ultima_alteracao",
    "load_date",
    "ingestion_timestamp",
    "source_file"
]

print("=" * 80)
print("STARTING SILVER PROCESSING: erp_orders")
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
        F.countDistinct("pedido_id").alias("distinct_pedido_ids"),
        F.sum(F.when(F.col("pedido_id").isNull(), 1).otherwise(0)).alias("null_pedido_id"),
        F.sum(F.when(F.col("data_pedido").isNull(), 1).otherwise(0)).alias("null_data_pedido"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id"),
        F.sum(F.when(F.col("canal_id").isNull(), 1).otherwise(0)).alias("null_canal_id"),
        F.sum(F.when(F.col("estado_pedido").isNull(), 1).otherwise(0)).alias("null_estado_pedido")
    )
    .collect()[0]
)

source_duplicate_pedido_ids = source_validation["row_count"] - source_validation["distinct_pedido_ids"]

print(f"Source row count:              {source_validation['row_count']:,}")
print(f"Repeated pedido_id records:     {source_duplicate_pedido_ids:,}")
print(f"Null pedido_id:                {source_validation['null_pedido_id']:,}")
print(f"Null data_pedido:              {source_validation['null_data_pedido']:,}")
print(f"Null cliente_id:               {source_validation['null_cliente_id']:,}")
print(f"Null canal_id:                 {source_validation['null_canal_id']:,}")
print(f"Null estado_pedido:            {source_validation['null_estado_pedido']:,}")

if source_validation["row_count"] == 0:
    raise ValueError("Source dataset is empty.")

source_null_failures = {
    "pedido_id": source_validation["null_pedido_id"],
    "data_pedido": source_validation["null_data_pedido"],
    "cliente_id": source_validation["null_cliente_id"],
    "canal_id": source_validation["null_canal_id"],
    "estado_pedido": source_validation["null_estado_pedido"]
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
    NULLIF(TRIM(pedido_id), '') AS pedido_id,

    COALESCE(
        TO_DATE(data_pedido, 'yyyy-MM-dd'),
        TO_DATE(data_pedido, 'dd/MM/yyyy'),
        TO_DATE(data_pedido, 'dd-MM-yyyy'),
        TO_DATE(data_pedido, 'yyyy/MM/dd')
    ) AS data_pedido,

    NULLIF(TRIM(cliente_id), '') AS cliente_id,
    NULLIF(TRIM(canal_id), '') AS canal_id,
    NULLIF(TRIM(vendedor_id), '') AS vendedor_id,
    NULLIF(TRIM(cidade_entrega), '') AS cidade_entrega,
    NULLIF(TRIM(estado_pedido), '') AS estado_pedido,
    CAST(prazo_entrega_dias AS INT) AS prazo_entrega_dias,
    NULLIF(TRIM(observacao_pedido), '') AS observacao_pedido,
    NULLIF(TRIM(sistema_origem), '') AS sistema_origem,
    NULLIF(TRIM(usuario_ultima_alteracao), '') AS usuario_ultima_alteracao,

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
        F.countDistinct("pedido_id").alias("distinct_pedido_ids"),
        F.sum(F.when(F.col("pedido_id").isNull(), 1).otherwise(0)).alias("null_pedido_id"),
        F.sum(F.when(F.col("data_pedido").isNull(), 1).otherwise(0)).alias("null_data_pedido"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id"),
        F.sum(F.when(F.col("canal_id").isNull(), 1).otherwise(0)).alias("null_canal_id"),
        F.sum(F.when(F.col("estado_pedido").isNull(), 1).otherwise(0)).alias("null_estado_pedido")
    )
    .collect()[0]
)

repeated_pedido_id_records = final["row_count"] - final["distinct_pedido_ids"]

print(f"Rows:                         {final['row_count']:,}")
print(f"Repeated pedido_id records:   {repeated_pedido_id_records:,}")
print(f"Null pedido_id:               {final['null_pedido_id']}")
print(f"Null data_pedido:             {final['null_data_pedido']}")
print(f"Null cliente_id:              {final['null_cliente_id']}")
print(f"Null canal_id:                {final['null_canal_id']}")
print(f"Null estado_pedido:           {final['null_estado_pedido']}")

if final["row_count"] == 0:
    raise ValueError("Silver dataset is empty.")

critical_nulls = {
    "pedido_id": final["null_pedido_id"],
    "data_pedido": final["null_data_pedido"],
    "cliente_id": final["null_cliente_id"],
    "canal_id": final["null_canal_id"],
    "estado_pedido": final["null_estado_pedido"]
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