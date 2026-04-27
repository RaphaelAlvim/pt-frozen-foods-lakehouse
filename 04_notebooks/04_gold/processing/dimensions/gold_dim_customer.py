# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_customer
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "dimensions"
DATASET = "dim_customer"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.silver_orders_customers"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_COLUMNS = [
    "status_cliente",
    "segmento"
]

REQUIRED_COLUMNS = [
    "cliente_id",
    "tipo_cliente",
    "segmento",
    "cliente_cidade",
    "distrito",
    "potencial_valor",
    "cluster_comercial",
    "status_cliente"
]

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_customer")
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

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

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
        F.countDistinct("cliente_id").alias("distinct_cliente_ids"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id")
    )
    .collect()[0]
)

print(f"Distinct cliente_id:     {source_validation['distinct_cliente_ids']:,}")
print(f"Null cliente_id:         {source_validation['null_cliente_id']:,}")

if source_validation["null_cliente_id"] > 0:
    raise ValueError("Null cliente_id detected in source dataset.")

print("[INFO] Source validation completed successfully.")

# ========================================
# 3. CREATE DIMENSION TABLE
# ========================================

print("[INFO] Creating Gold dimension table using CTAS...")

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
    cliente_id,
    tipo_cliente,
    segmento,
    cliente_cidade,
    distrito,
    potencial_valor,
    cluster_comercial,
    status_cliente

FROM {SOURCE_TABLE}
""")

print("[INFO] Gold dimension table created successfully.")

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
        F.countDistinct("cliente_id").alias("distinct_ids"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id"),
        F.sum(F.when(F.col("tipo_cliente").isNull(), 1).otherwise(0)).alias("null_tipo_cliente"),
        F.sum(F.when(F.col("segmento").isNull(), 1).otherwise(0)).alias("null_segmento"),
        F.sum(F.when(F.col("cliente_cidade").isNull(), 1).otherwise(0)).alias("null_cliente_cidade"),
        F.sum(F.when(F.col("distrito").isNull(), 1).otherwise(0)).alias("null_distrito"),
        F.sum(F.when(F.col("potencial_valor").isNull(), 1).otherwise(0)).alias("null_potencial_valor"),
        F.sum(F.when(F.col("cluster_comercial").isNull(), 1).otherwise(0)).alias("null_cluster_comercial"),
        F.sum(F.when(F.col("status_cliente").isNull(), 1).otherwise(0)).alias("null_status_cliente")
    )
    .collect()[0]
)

duplicates = final["row_count"] - final["distinct_ids"]

print(f"Expected rows:              {source_validation['distinct_cliente_ids']:,}")
print(f"Rows:                       {final['row_count']:,}")
print(f"Duplicates:                 {duplicates:,}")
print(f"Null cliente_id:            {final['null_cliente_id']}")
print(f"Null tipo_cliente:          {final['null_tipo_cliente']}")
print(f"Null segmento:              {final['null_segmento']}")
print(f"Null cliente_cidade:        {final['null_cliente_cidade']}")
print(f"Null distrito:              {final['null_distrito']}")
print(f"Null potencial_valor:       {final['null_potencial_valor']}")
print(f"Null cluster_comercial:     {final['null_cluster_comercial']}")
print(f"Null status_cliente:        {final['null_status_cliente']}")

if final["row_count"] != source_validation["distinct_cliente_ids"]:
    raise ValueError("Row count mismatch detected.")

if duplicates > 0:
    raise ValueError("Duplicate cliente_id detected.")

critical_nulls = {
    "cliente_id": final["null_cliente_id"],
    "tipo_cliente": final["null_tipo_cliente"],
    "segmento": final["null_segmento"],
    "cliente_cidade": final["null_cliente_cidade"],
    "distrito": final["null_distrito"],
    "potencial_valor": final["null_potencial_valor"],
    "cluster_comercial": final["null_cluster_comercial"],
    "status_cliente": final["null_status_cliente"]
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