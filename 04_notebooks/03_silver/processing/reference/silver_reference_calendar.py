# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: reference_calendar
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

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

CLUSTER_COLUMNS = [
    "data",
    "ano",
    "mes"
]

REQUIRED_COLUMNS = [
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
]

print("=" * 80)
print("STARTING SILVER PROCESSING: reference_calendar")
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
        F.sum(F.when(F.col("data").isNull(), 1).otherwise(0)).alias("null_data")
    )
    .collect()[0]
)

print(f"Source row count:     {source_validation['row_count']:,}")
print(f"Null data:            {source_validation['null_data']:,}")

if source_validation["row_count"] == 0:
    raise ValueError("Source dataset is empty.")

if source_validation["null_data"] > 0:
    raise ValueError(f"Null values detected in source critical columns: {{'data': {source_validation['null_data']}}}")

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
    COALESCE(
        TO_DATE(data, 'yyyy-MM-dd'),
        TO_DATE(data, 'yyyy/MM/dd'),
        TO_DATE(data, 'dd/MM/yyyy'),
        TO_DATE(data, 'dd-MM-yyyy')
    ) AS data,

    CAST(ano AS INT) AS ano,
    CAST(mes AS INT) AS mes,
    CAST(dia AS INT) AS dia,
    CAST(trimestre AS INT) AS trimestre,

    NULLIF(TRIM(nome_mes), '') AS nome_mes,
    CAST(dia_semana AS INT) AS dia_semana,
    NULLIF(TRIM(nome_dia_semana), '') AS nome_dia_semana,

    CAST(is_fim_de_semana AS BOOLEAN) AS is_fim_de_semana,
    CAST(is_inicio_mes AS BOOLEAN) AS is_inicio_mes,
    CAST(is_fim_mes AS BOOLEAN) AS is_fim_mes,

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
        F.countDistinct("data").alias("distinct_dates"),
        F.sum(F.when(F.col("data").isNull(), 1).otherwise(0)).alias("null_data"),
        F.sum(F.when((F.col("mes") < 1) | (F.col("mes") > 12), 1).otherwise(0)).alias("invalid_month"),
        F.sum(F.when((F.col("dia") < 1) | (F.col("dia") > 31), 1).otherwise(0)).alias("invalid_day"),
        F.sum(F.when((F.col("trimestre") < 1) | (F.col("trimestre") > 4), 1).otherwise(0)).alias("invalid_quarter"),
        F.sum(
            F.when(
                (F.year("data") != F.col("ano")) |
                (F.month("data") != F.col("mes")) |
                (F.dayofmonth("data") != F.col("dia")),
                1
            ).otherwise(0)
        ).alias("date_mismatch")
    )
    .collect()[0]
)

duplicate_date_records = final["row_count"] - final["distinct_dates"]

print(f"Rows:                    {final['row_count']:,}")
print(f"Duplicate date records:  {duplicate_date_records:,}")
print(f"Null data:               {final['null_data']}")
print(f"Invalid month:           {final['invalid_month']}")
print(f"Invalid day:             {final['invalid_day']}")
print(f"Invalid quarter:         {final['invalid_quarter']}")
print(f"Date mismatch:           {final['date_mismatch']}")

if final["row_count"] == 0:
    raise ValueError("Silver dataset is empty.")

if final["null_data"] > 0:
    raise ValueError(f"Null values detected in critical columns: {{'data': {final['null_data']}}}")

if final["invalid_month"] > 0:
    raise ValueError(f"Invalid month values detected: {final['invalid_month']}")

if final["invalid_day"] > 0:
    raise ValueError(f"Invalid day values detected: {final['invalid_day']}")

if final["invalid_quarter"] > 0:
    raise ValueError(f"Invalid quarter values detected: {final['invalid_quarter']}")

if final["date_mismatch"] > 0:
    raise ValueError(f"Date decomposition mismatch detected: {final['date_mismatch']}")

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