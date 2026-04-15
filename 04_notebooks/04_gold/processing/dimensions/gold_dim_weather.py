# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_weather
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "dimensions"
DATASET = "dim_weather"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

WEATHER_DATASET = "weather_porto_daily"

WEATHER_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{WEATHER_DATASET}"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_KEYS = ["weather_date", "city"]

AUTO_OPTIMIZE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}


# ========================================
# 1. CONTEXT SETUP
# ========================================

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_weather")
print("=" * 80)

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print("[INFO] Context setup completed successfully.")


# ========================================
# 2. CONFIGURATION SUMMARY
# ========================================

print("=" * 80)
print("GOLD PROCESSING NOTEBOOK CONFIGURATION")
print("=" * 80)
print(f"Catalog:                         {CATALOG}")
print(f"Source schema:                   {SOURCE_SCHEMA}")
print(f"Target schema:                   {TARGET_SCHEMA}")
print(f"Domain:                          {DOMAIN}")
print(f"Dataset:                         {DATASET}")
print(f"Weather table:                   {WEATHER_TABLE}")
print(f"Target table:                    {TARGET_TABLE}")
print(f"Target path:                     {TARGET_PATH}")
print(f"Cluster keys:                    {', '.join(CLUSTER_KEYS)}")
print(f"Optimize write enabled:          {AUTO_OPTIMIZE_PROPERTIES['delta.autoOptimize.optimizeWrite']}")
print(f"Auto compact enabled:            {AUTO_OPTIMIZE_PROPERTIES['delta.autoOptimize.autoCompact']}")
print("=" * 80)


# ========================================
# 3. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {WEATHER_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")


# ========================================
# 4. READ SOURCE DATA
# ========================================

df_weather = spark.table(WEATHER_TABLE)

print("[INFO] Source data loaded successfully.")
print(f"[INFO] Weather row count:                    {df_weather.count():,}")


# ========================================
# 5. SOURCE VALIDATION
# ========================================

print("[INFO] Validating source datasets...")

raw_weather_count = df_weather.count()
distinct_date_city_count = df_weather.select("data", "cidade").distinct().count()
null_date_count = df_weather.filter(F.col("data").isNull()).count()
null_city_count = df_weather.filter(F.col("cidade").isNull()).count()

required_columns = [
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

missing_columns = [c for c in required_columns if c not in df_weather.columns]

print(f"[INFO] Distinct date-city combinations:      {distinct_date_city_count:,}")
print(f"[INFO] Null data count:                      {null_date_count:,}")
print(f"[INFO] Null cidade count:                    {null_city_count:,}")
print(f"[INFO] Missing required columns:             {missing_columns}")

if distinct_date_city_count != raw_weather_count:
    raise ValueError(
        f"Duplicate date-city combinations detected in source dataset. Distinct: {distinct_date_city_count}, Rows: {raw_weather_count}"
    )

if null_date_count > 0:
    raise ValueError(f"Null data detected in source dataset: {null_date_count}")

if null_city_count > 0:
    raise ValueError(f"Null cidade detected in source dataset: {null_city_count}")

if missing_columns:
    raise ValueError(f"Missing required columns in source dataset: {missing_columns}")

print("[INFO] Source validation completed successfully.")


# ========================================
# 6. BUILD DIMENSION DATASET
# ========================================

print("[INFO] Building Gold dimension dataset with explicit column selection...")

df_dim_weather = (
    df_weather
    .select(
        F.col("data").alias("weather_date"),
        F.col("cidade").alias("city"),
        F.col("temperatura_media").alias("avg_temperature"),
        F.col("temperatura_min").alias("min_temperature"),
        F.col("temperatura_max").alias("max_temperature"),
        F.col("choveu").alias("did_rain"),
        F.col("precipitacao_mm").alias("precipitation_mm"),
        F.col("humidade_media").alias("avg_humidity"),
        F.col("vento_kmh").alias("wind_kmh"),
        F.col("fonte_api").alias("weather_source")
    )
    .dropDuplicates(["weather_date", "city"])
)

print("[INFO] Gold dimension dataset built successfully.")
print(f"[INFO] Row count after build:                {df_dim_weather.count():,}")


# ========================================
# 7. OUTPUT VALIDATION
# ========================================

print("[INFO] Validating Gold dimension output...")

expected_row_count = distinct_date_city_count
final_row_count = df_dim_weather.count()

duplicate_date_city_count = (
    df_dim_weather
    .groupBy("weather_date", "city")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_weather_date_final = df_dim_weather.filter(F.col("weather_date").isNull()).count()
null_city_final = df_dim_weather.filter(F.col("city").isNull()).count()
null_avg_temperature_final = df_dim_weather.filter(F.col("avg_temperature").isNull()).count()
null_min_temperature_final = df_dim_weather.filter(F.col("min_temperature").isNull()).count()
null_max_temperature_final = df_dim_weather.filter(F.col("max_temperature").isNull()).count()
null_did_rain_final = df_dim_weather.filter(F.col("did_rain").isNull()).count()
null_precipitation_final = df_dim_weather.filter(F.col("precipitation_mm").isNull()).count()
null_avg_humidity_final = df_dim_weather.filter(F.col("avg_humidity").isNull()).count()
null_wind_kmh_final = df_dim_weather.filter(F.col("wind_kmh").isNull()).count()
null_weather_source_final = df_dim_weather.filter(F.col("weather_source").isNull()).count()

print(f"[INFO] Expected row count:                   {expected_row_count:,}")
print(f"[INFO] Output row count:                     {final_row_count:,}")
print(f"[INFO] Duplicate date-city count:           {duplicate_date_city_count:,}")
print(f"[INFO] Null weather_date count:             {null_weather_date_final:,}")
print(f"[INFO] Null city count:                     {null_city_final:,}")
print(f"[INFO] Null avg_temperature count:          {null_avg_temperature_final:,}")
print(f"[INFO] Null min_temperature count:          {null_min_temperature_final:,}")
print(f"[INFO] Null max_temperature count:          {null_max_temperature_final:,}")
print(f"[INFO] Null did_rain count:                 {null_did_rain_final:,}")
print(f"[INFO] Null precipitation_mm count:         {null_precipitation_final:,}")
print(f"[INFO] Null avg_humidity count:             {null_avg_humidity_final:,}")
print(f"[INFO] Null wind_kmh count:                 {null_wind_kmh_final:,}")
print(f"[INFO] Null weather_source count:           {null_weather_source_final:,}")

if final_row_count != expected_row_count:
    raise ValueError(
        f"Row count mismatch detected. Expected: {expected_row_count}, Got: {final_row_count}"
    )

if duplicate_date_city_count > 0:
    raise ValueError(f"Duplicate weather_date-city detected in output dataset: {duplicate_date_city_count}")

if null_weather_date_final > 0:
    raise ValueError(f"Null weather_date detected in output dataset: {null_weather_date_final}")

if null_city_final > 0:
    raise ValueError(f"Null city detected in output dataset: {null_city_final}")

if null_avg_temperature_final > 0:
    raise ValueError(f"Null avg_temperature detected in output dataset: {null_avg_temperature_final}")

if null_min_temperature_final > 0:
    raise ValueError(f"Null min_temperature detected in output dataset: {null_min_temperature_final}")

if null_max_temperature_final > 0:
    raise ValueError(f"Null max_temperature detected in output dataset: {null_max_temperature_final}")

if null_did_rain_final > 0:
    raise ValueError(f"Null did_rain detected in output dataset: {null_did_rain_final}")

if null_precipitation_final > 0:
    raise ValueError(f"Null precipitation_mm detected in output dataset: {null_precipitation_final}")

if null_avg_humidity_final > 0:
    raise ValueError(f"Null avg_humidity detected in output dataset: {null_avg_humidity_final}")

if null_wind_kmh_final > 0:
    raise ValueError(f"Null wind_kmh detected in output dataset: {null_wind_kmh_final}")

if null_weather_source_final > 0:
    raise ValueError(f"Null weather_source detected in output dataset: {null_weather_source_final}")

print("[INFO] Output validation completed successfully.")


# ========================================
# 8. WRITE DELTA TABLE
# ========================================

print("[INFO] Writing Gold dimension table to Delta format...")

(
    df_dim_weather.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(TARGET_PATH)
)

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

spark.sql(f"""
    CREATE TABLE {TARGET_TABLE}
    USING DELTA
    LOCATION '{TARGET_PATH}'
""")

print("[INFO] Delta table written successfully.")


# ========================================
# 9. APPLY TABLE OPTIMIZATION
# ========================================

print("[INFO] Applying Auto Optimize table properties...")

for property_name, property_value in AUTO_OPTIMIZE_PROPERTIES.items():
    spark.sql(f"""
        ALTER TABLE {TARGET_TABLE}
        SET TBLPROPERTIES ('{property_name}' = '{property_value}')
    """)

print("[INFO] Auto Optimize table properties applied successfully.")

print("[INFO] Applying Liquid Clustering...")
spark.sql(f"ALTER TABLE {TARGET_TABLE} CLUSTER BY ({', '.join(CLUSTER_KEYS)})")
print("[INFO] Liquid clustering applied successfully.")


# ========================================
# 10. FINAL STATUS
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