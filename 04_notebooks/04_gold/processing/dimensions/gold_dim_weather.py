# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_weather
# ========================================

# Processing decisions:
# - Use CTAS (CREATE OR REPLACE TABLE AS SELECT) to create and register the Delta table in one step.
# - Use Liquid Clustering by weather_date and city to support analytical filters.
# - Avoid unnecessary cache, repartition, or coalesce operations.
# - Consolidate validation checks to reduce unnecessary scans.
# - Keep critical grain and null validations to protect dimension quality.

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "dimensions"
DATASET = "dim_weather"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.weather_porto_daily"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

GRAIN_COLUMNS = [
    "weather_date",
    "city"
]

CLUSTER_COLUMNS = [
    "weather_date",
    "city"
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
    "fonte_api"
]

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_weather")
print("=" * 80)

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print("[INFO] Context setup completed successfully.")

# ========================================
# 1. CONFIGURATION SUMMARY
# ========================================

print("=" * 80)
print("GOLD PROCESSING NOTEBOOK CONFIGURATION")
print("=" * 80)
print(f"Catalog:                         {CATALOG}")
print(f"Source schema:                   {SOURCE_SCHEMA}")
print(f"Target schema:                   {TARGET_SCHEMA}")
print(f"Domain:                          {DOMAIN}")
print(f"Dataset:                         {DATASET}")
print(f"Source table:                    {SOURCE_TABLE}")
print(f"Target table:                    {TARGET_TABLE}")
print(f"Target path:                     {TARGET_PATH}")
print(f"Grain columns:                   {', '.join(GRAIN_COLUMNS)}")
print(f"Cluster columns:                 {', '.join(CLUSTER_COLUMNS)}")
print(f"Optimization strategy:           Delta Auto Optimize + Liquid Clustering")
print(f"Partitioning strategy:           None")
print("=" * 80)

# ========================================
# 2. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {SOURCE_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")

# ========================================
# 3. SOURCE VALIDATION
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
        F.countDistinct("data", "cidade").alias("distinct_date_city_count"),
        F.sum(F.when(F.col("data").isNull(), 1).otherwise(0)).alias("null_data"),
        F.sum(F.when(F.col("cidade").isNull(), 1).otherwise(0)).alias("null_cidade")
    )
    .collect()[0]
)

print(f"Source row count:                  {source_validation['row_count']:,}")
print(f"Distinct date-city combinations:   {source_validation['distinct_date_city_count']:,}")
print(f"Null data count:                   {source_validation['null_data']:,}")
print(f"Null cidade count:                 {source_validation['null_cidade']:,}")

if source_validation["distinct_date_city_count"] != source_validation["row_count"]:
    raise ValueError(
        f"Duplicate date-city combinations detected in source dataset. "
        f"Distinct: {source_validation['distinct_date_city_count']}, Rows: {source_validation['row_count']}"
    )

if source_validation["null_data"] > 0:
    raise ValueError(f"Null data detected in source dataset: {source_validation['null_data']}")

if source_validation["null_cidade"] > 0:
    raise ValueError(f"Null cidade detected in source dataset: {source_validation['null_cidade']}")

print("[INFO] Source validation completed successfully.")

# ========================================
# 4. CREATE DIMENSION TABLE
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
    data AS weather_date,
    cidade AS city,
    temperatura_media AS avg_temperature,
    temperatura_min AS min_temperature,
    temperatura_max AS max_temperature,
    choveu AS did_rain,
    precipitacao_mm AS precipitation_mm,
    humidade_media AS avg_humidity,
    vento_kmh AS wind_kmh,
    fonte_api AS weather_source

FROM {SOURCE_TABLE}
""")

print("[INFO] Gold dimension table created successfully.")

# ========================================
# 5. OPTIMIZATION
# ========================================

# OPTIMIZE is executed after the full rebuild because this dimension may support
# recurring analytical joins and filters.
# In future production scenarios, this may be moved to a scheduled maintenance
# job if execution cost becomes relevant.

print("[INFO] Running OPTIMIZE for clustered Delta layout...")

spark.sql(f"OPTIMIZE {TARGET_TABLE}")

print("[INFO] Table optimization completed.")

# ========================================
# 6. FINAL VALIDATIONS
# ========================================

print("=" * 80)
print("FINAL VALIDATIONS")
print("=" * 80)

df_target = spark.table(TARGET_TABLE)

validation = (
    df_target
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("weather_date", "city").alias("distinct_grain_rows"),
        F.sum(F.when(F.col("weather_date").isNull(), 1).otherwise(0)).alias("null_weather_date"),
        F.sum(F.when(F.col("city").isNull(), 1).otherwise(0)).alias("null_city"),
        F.sum(F.when(F.col("avg_temperature").isNull(), 1).otherwise(0)).alias("null_avg_temperature"),
        F.sum(F.when(F.col("min_temperature").isNull(), 1).otherwise(0)).alias("null_min_temperature"),
        F.sum(F.when(F.col("max_temperature").isNull(), 1).otherwise(0)).alias("null_max_temperature"),
        F.sum(F.when(F.col("did_rain").isNull(), 1).otherwise(0)).alias("null_did_rain"),
        F.sum(F.when(F.col("precipitation_mm").isNull(), 1).otherwise(0)).alias("null_precipitation_mm"),
        F.sum(F.when(F.col("avg_humidity").isNull(), 1).otherwise(0)).alias("null_avg_humidity"),
        F.sum(F.when(F.col("wind_kmh").isNull(), 1).otherwise(0)).alias("null_wind_kmh"),
        F.sum(F.when(F.col("weather_source").isNull(), 1).otherwise(0)).alias("null_weather_source")
    )
    .collect()[0]
)

duplicate_rows = validation["row_count"] - validation["distinct_grain_rows"]

print(f"Expected row count:              {source_validation['distinct_date_city_count']:,}")
print(f"Output row count:                {validation['row_count']:,}")
print(f"Duplicate grain rows:            {duplicate_rows:,}")
print(f"Null weather_date count:         {validation['null_weather_date']:,}")
print(f"Null city count:                 {validation['null_city']:,}")
print(f"Null avg_temperature count:      {validation['null_avg_temperature']:,}")
print(f"Null min_temperature count:      {validation['null_min_temperature']:,}")
print(f"Null max_temperature count:      {validation['null_max_temperature']:,}")
print(f"Null did_rain count:             {validation['null_did_rain']:,}")
print(f"Null precipitation_mm count:     {validation['null_precipitation_mm']:,}")
print(f"Null avg_humidity count:         {validation['null_avg_humidity']:,}")
print(f"Null wind_kmh count:             {validation['null_wind_kmh']:,}")
print(f"Null weather_source count:       {validation['null_weather_source']:,}")

if validation["row_count"] != source_validation["distinct_date_city_count"]:
    raise ValueError(
        f"Row count mismatch detected. Expected: {source_validation['distinct_date_city_count']}, "
        f"Got: {validation['row_count']}"
    )

if duplicate_rows > 0:
    raise ValueError(f"Duplicate weather_date-city detected in output dataset: {duplicate_rows}")

critical_nulls = {
    "weather_date": validation["null_weather_date"],
    "city": validation["null_city"],
    "avg_temperature": validation["null_avg_temperature"],
    "min_temperature": validation["null_min_temperature"],
    "max_temperature": validation["null_max_temperature"],
    "did_rain": validation["null_did_rain"],
    "precipitation_mm": validation["null_precipitation_mm"],
    "avg_humidity": validation["null_avg_humidity"],
    "wind_kmh": validation["null_wind_kmh"],
    "weather_source": validation["null_weather_source"]
}

null_failures = {column: count for column, count in critical_nulls.items() if count > 0}

if null_failures:
    raise ValueError(f"Null values detected in critical columns: {null_failures}")

print("[INFO] Final validations completed successfully.")

# ========================================
# 7. FINAL TABLE DETAIL
# ========================================

print("=" * 80)
print("FINAL TABLE DETAIL")
print("=" * 80)

final_detail = spark.sql(f"DESCRIBE DETAIL {TARGET_TABLE}").collect()[0].asDict()

print(f"Format:              {final_detail.get('format')}")
print(f"Table name:          {final_detail.get('name')}")
print(f"Location:            {final_detail.get('location')}")
print(f"Created at:          {final_detail.get('createdAt')}")
print(f"Last modified:       {final_detail.get('lastModified')}")
print(f"Partition columns:   {final_detail.get('partitionColumns')}")
print(f"Clustering columns:  {final_detail.get('clusteringColumns')}")
print(f"Number of files:     {final_detail.get('numFiles')}")
print(f"Size in bytes:       {final_detail.get('sizeInBytes')}")

print("=" * 80)
print("GOLD PROCESSING COMPLETED SUCCESSFULLY")
print("=" * 80)
print(f"Target table: {TARGET_TABLE}")
print(f"Target path:  {TARGET_PATH}")