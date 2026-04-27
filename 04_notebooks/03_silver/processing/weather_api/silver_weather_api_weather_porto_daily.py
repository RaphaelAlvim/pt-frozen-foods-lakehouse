# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: weather_porto_daily
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

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

CLUSTER_COLUMNS = [
    "data",
    "cidade"
]

REQUIRED_COLUMNS = [
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
]

print("=" * 80)
print("STARTING SILVER PROCESSING: weather_porto_daily")
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
        F.sum(F.when(F.col("data").isNull(), 1).otherwise(0)).alias("null_data"),
        F.sum(F.when(F.col("cidade").isNull(), 1).otherwise(0)).alias("null_cidade"),
        F.sum(F.when(F.col("fonte_api").isNull(), 1).otherwise(0)).alias("null_fonte_api")
    )
    .collect()[0]
)

print(f"Source row count:      {source_validation['row_count']:,}")
print(f"Null data:             {source_validation['null_data']:,}")
print(f"Null cidade:           {source_validation['null_cidade']:,}")
print(f"Null fonte_api:        {source_validation['null_fonte_api']:,}")

if source_validation["row_count"] == 0:
    raise ValueError("Source dataset is empty.")

source_null_failures = {
    "data": source_validation["null_data"],
    "cidade": source_validation["null_cidade"],
    "fonte_api": source_validation["null_fonte_api"]
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
    COALESCE(
        TO_DATE(data, 'yyyy-MM-dd'),
        TO_DATE(data, 'yyyy/MM/dd'),
        TO_DATE(data, 'dd/MM/yyyy'),
        TO_DATE(data, 'dd-MM-yyyy')
    ) AS data,

    NULLIF(TRIM(cidade), '') AS cidade,

    CAST(temperatura_media AS DECIMAL(18,4)) AS temperatura_media,
    CAST(temperatura_min AS DECIMAL(18,4)) AS temperatura_min,
    CAST(temperatura_max AS DECIMAL(18,4)) AS temperatura_max,
    CAST(choveu AS INT) AS choveu,
    CAST(precipitacao_mm AS DECIMAL(18,4)) AS precipitacao_mm,
    CAST(humidade_media AS DECIMAL(18,4)) AS humidade_media,
    CAST(vento_kmh AS DECIMAL(18,4)) AS vento_kmh,

    NULLIF(TRIM(fonte_api), '') AS fonte_api,

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
        F.sum(F.when(F.col("cidade").isNull(), 1).otherwise(0)).alias("null_cidade"),
        F.sum(F.when(F.col("fonte_api").isNull(), 1).otherwise(0)).alias("null_fonte_api"),
        F.sum(F.when(~F.col("choveu").isin(VALID_CHOVEU_VALUES), 1).otherwise(0)).alias("invalid_choveu"),
        F.sum(F.when((F.col("humidade_media") < 0) | (F.col("humidade_media") > 100), 1).otherwise(0)).alias("invalid_humidade"),
        F.sum(F.when(F.col("precipitacao_mm") < 0, 1).otherwise(0)).alias("invalid_precipitacao"),
        F.sum(F.when(F.col("vento_kmh") < 0, 1).otherwise(0)).alias("invalid_vento"),
        F.sum(
            F.when(
                (F.col("temperatura_min") > F.col("temperatura_media")) |
                (F.col("temperatura_media") > F.col("temperatura_max")) |
                (F.col("temperatura_min") > F.col("temperatura_max")),
                1
            ).otherwise(0)
        ).alias("invalid_temperatura_interval"),
        F.sum(F.when(F.col("cidade") != EXPECTED_CITY, 1).otherwise(0)).alias("invalid_cidade")
    )
    .collect()[0]
)

duplicate_date_records = final["row_count"] - final["distinct_dates"]

print(f"Rows:                         {final['row_count']:,}")
print(f"Duplicate date records:       {duplicate_date_records:,}")
print(f"Null data:                    {final['null_data']}")
print(f"Null cidade:                  {final['null_cidade']}")
print(f"Null fonte_api:               {final['null_fonte_api']}")
print(f"Invalid choveu:               {final['invalid_choveu']}")
print(f"Invalid humidade_media:       {final['invalid_humidade']}")
print(f"Invalid precipitacao_mm:      {final['invalid_precipitacao']}")
print(f"Invalid vento_kmh:            {final['invalid_vento']}")
print(f"Invalid temperatura interval: {final['invalid_temperatura_interval']}")
print(f"Invalid cidade:               {final['invalid_cidade']}")

if final["row_count"] == 0:
    raise ValueError("Silver dataset is empty.")

critical_nulls = {
    "data": final["null_data"],
    "cidade": final["null_cidade"],
    "fonte_api": final["null_fonte_api"]
}

null_failures = {column: count for column, count in critical_nulls.items() if count > 0}

if null_failures:
    raise ValueError(f"Null values detected in critical columns: {null_failures}")

if duplicate_date_records > 0:
    raise ValueError(f"Duplicate date detected after transformation: {duplicate_date_records}")

if final["invalid_choveu"] > 0:
    raise ValueError(f"Invalid choveu values detected: {final['invalid_choveu']}")

if final["invalid_humidade"] > 0:
    raise ValueError(f"Invalid humidade_media values detected: {final['invalid_humidade']}")

if final["invalid_precipitacao"] > 0:
    raise ValueError(f"Invalid precipitacao_mm values detected: {final['invalid_precipitacao']}")

if final["invalid_vento"] > 0:
    raise ValueError(f"Invalid vento_kmh values detected: {final['invalid_vento']}")

if final["invalid_temperatura_interval"] > 0:
    raise ValueError(f"Temperature interval validation failed: {final['invalid_temperatura_interval']}")

if final["invalid_cidade"] > 0:
    raise ValueError(f"Unexpected cidade values different from {EXPECTED_CITY}: {final['invalid_cidade']}")

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