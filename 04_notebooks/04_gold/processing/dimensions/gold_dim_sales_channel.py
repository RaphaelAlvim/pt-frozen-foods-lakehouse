# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_sales_channel
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "dimensions"
DATASET = "dim_sales_channel"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.reference_sales_channels"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_COLUMNS = ["nome_canal"]

REQUIRED_COLUMNS = [
    "canal_id",
    "nome_canal",
    "descricao"
]

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_sales_channel")
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

validation = (
    df_source
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("canal_id").alias("distinct_ids"),
        F.sum(F.when(F.col("canal_id").isNull(), 1).otherwise(0)).alias("null_id")
    )
    .collect()[0]
)

print(f"Source row count:          {validation['row_count']:,}")
print(f"Distinct canal_id:         {validation['distinct_ids']:,}")
print(f"Null canal_id:             {validation['null_id']:,}")

if validation["row_count"] != validation["distinct_ids"]:
    raise ValueError("canal_id is not unique.")

if validation["null_id"] > 0:
    raise ValueError("Null canal_id detected.")

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
SELECT
    canal_id,
    nome_canal,
    descricao
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
        F.countDistinct("canal_id").alias("distinct_ids"),
        F.sum(F.when(F.col("canal_id").isNull(), 1).otherwise(0)).alias("null_id"),
        F.sum(F.when(F.col("nome_canal").isNull(), 1).otherwise(0)).alias("null_nome"),
        F.sum(F.when(F.col("descricao").isNull(), 1).otherwise(0)).alias("null_desc")
    )
    .collect()[0]
)

duplicates = final["row_count"] - final["distinct_ids"]

print(f"Rows:               {final['row_count']:,}")
print(f"Duplicates:         {duplicates:,}")
print(f"Null canal_id:      {final['null_id']}")
print(f"Null nome_canal:    {final['null_nome']}")
print(f"Null descricao:     {final['null_desc']}")

if duplicates > 0:
    raise ValueError("Duplicate canal_id detected.")

if final["null_id"] > 0:
    raise ValueError("Null canal_id detected.")

if final["null_nome"] > 0:
    raise ValueError("Null nome_canal detected.")

if final["null_desc"] > 0:
    raise ValueError("Null descricao detected.")

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