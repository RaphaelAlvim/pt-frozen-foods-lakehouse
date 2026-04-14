# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_calendar
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "dimensions"
DATASET = "dim_calendar"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

CALENDAR_DATASET = "reference_calendar"

CALENDAR_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{CALENDAR_DATASET}"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_KEYS = ["calendar_year", "calendar_month"]

AUTO_OPTIMIZE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}


# ========================================
# 1. CONTEXT SETUP
# ========================================

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_calendar")
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
print(f"Calendar table:                  {CALENDAR_TABLE}")
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
spark.sql(f"DESCRIBE TABLE {CALENDAR_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")


# ========================================
# 4. READ SOURCE DATA
# ========================================

df_calendar = spark.table(CALENDAR_TABLE)

print("[INFO] Source data loaded successfully.")
print(f"[INFO] Calendar row count:                   {df_calendar.count():,}")


# ========================================
# 5. SOURCE VALIDATION
# ========================================

print("[INFO] Validating source datasets...")

required_columns = [
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
    "is_fim_mes"
]

missing_columns = [c for c in required_columns if c not in df_calendar.columns]
null_calendar_date = df_calendar.filter(F.col("data").isNull()).count()
distinct_dates = df_calendar.select("data").distinct().count()
raw_row_count = df_calendar.count()

print(f"[INFO] Distinct dates count:                {distinct_dates:,}")
print(f"[INFO] Null calendar date count:           {null_calendar_date:,}")
print(f"[INFO] Missing required columns:           {missing_columns}")

if missing_columns:
    raise ValueError(f"Missing required columns in source dataset: {missing_columns}")

if null_calendar_date > 0:
    raise ValueError(f"Null data detected in source dataset: {null_calendar_date}")

if distinct_dates != raw_row_count:
    raise ValueError(
        f"Duplicate calendar dates detected. Distinct: {distinct_dates}, Rows: {raw_row_count}"
    )

print("[INFO] Source validation completed successfully.")


# ========================================
# 6. BUILD DIMENSION DATASET
# ========================================

print("[INFO] Building Gold dimension dataset with explicit column selection...")

df_dim_calendar = (
    df_calendar
    .select(
        F.col("data").alias("calendar_date"),
        F.col("ano").alias("calendar_year"),
        F.col("mes").alias("calendar_month"),
        F.col("dia").alias("calendar_day"),
        F.col("trimestre").alias("calendar_quarter"),
        F.col("nome_mes").alias("calendar_month_name"),
        F.col("dia_semana").alias("calendar_day_of_week"),
        F.col("nome_dia_semana").alias("calendar_day_name"),
        F.col("is_fim_de_semana").alias("calendar_is_weekend"),
        F.col("is_inicio_mes").alias("calendar_is_month_start"),
        F.col("is_fim_mes").alias("calendar_is_month_end")
    )
)

print("[INFO] Gold dimension dataset built successfully.")
print(f"[INFO] Row count after build:                {df_dim_calendar.count():,}")


# ========================================
# 7. OUTPUT VALIDATION
# ========================================

print("[INFO] Validating Gold dimension output...")

final_row_count = df_dim_calendar.count()
expected_row_count = raw_row_count

duplicate_calendar_dates = (
    df_dim_calendar
    .groupBy("calendar_date")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_calendar_date_final = df_dim_calendar.filter(F.col("calendar_date").isNull()).count()
null_calendar_year = df_dim_calendar.filter(F.col("calendar_year").isNull()).count()
null_calendar_month = df_dim_calendar.filter(F.col("calendar_month").isNull()).count()
null_calendar_day = df_dim_calendar.filter(F.col("calendar_day").isNull()).count()
null_calendar_quarter = df_dim_calendar.filter(F.col("calendar_quarter").isNull()).count()
null_calendar_month_name = df_dim_calendar.filter(F.col("calendar_month_name").isNull()).count()
null_calendar_day_of_week = df_dim_calendar.filter(F.col("calendar_day_of_week").isNull()).count()
null_calendar_day_name = df_dim_calendar.filter(F.col("calendar_day_name").isNull()).count()
null_calendar_is_weekend = df_dim_calendar.filter(F.col("calendar_is_weekend").isNull()).count()
null_calendar_is_month_start = df_dim_calendar.filter(F.col("calendar_is_month_start").isNull()).count()
null_calendar_is_month_end = df_dim_calendar.filter(F.col("calendar_is_month_end").isNull()).count()

print(f"[INFO] Expected row count:                   {expected_row_count:,}")
print(f"[INFO] Output row count:                     {final_row_count:,}")
print(f"[INFO] Duplicate calendar_date count:       {duplicate_calendar_dates:,}")
print(f"[INFO] Null calendar_date count:            {null_calendar_date_final:,}")
print(f"[INFO] Null calendar_year count:            {null_calendar_year:,}")
print(f"[INFO] Null calendar_month count:           {null_calendar_month:,}")
print(f"[INFO] Null calendar_day count:             {null_calendar_day:,}")
print(f"[INFO] Null calendar_quarter count:         {null_calendar_quarter:,}")
print(f"[INFO] Null calendar_month_name count:      {null_calendar_month_name:,}")
print(f"[INFO] Null calendar_day_of_week count:     {null_calendar_day_of_week:,}")
print(f"[INFO] Null calendar_day_name count:        {null_calendar_day_name:,}")
print(f"[INFO] Null calendar_is_weekend count:      {null_calendar_is_weekend:,}")
print(f"[INFO] Null calendar_is_month_start count:  {null_calendar_is_month_start:,}")
print(f"[INFO] Null calendar_is_month_end count:    {null_calendar_is_month_end:,}")

if final_row_count != expected_row_count:
    raise ValueError(
        f"Row count mismatch detected. Expected: {expected_row_count}, Got: {final_row_count}"
    )

if duplicate_calendar_dates > 0:
    raise ValueError(f"Duplicate calendar_date detected in output dataset: {duplicate_calendar_dates}")

if null_calendar_date_final > 0:
    raise ValueError(f"Null calendar_date detected in output dataset: {null_calendar_date_final}")

if null_calendar_year > 0:
    raise ValueError(f"Null calendar_year detected in output dataset: {null_calendar_year}")

if null_calendar_month > 0:
    raise ValueError(f"Null calendar_month detected in output dataset: {null_calendar_month}")

if null_calendar_day > 0:
    raise ValueError(f"Null calendar_day detected in output dataset: {null_calendar_day}")

if null_calendar_quarter > 0:
    raise ValueError(f"Null calendar_quarter detected in output dataset: {null_calendar_quarter}")

if null_calendar_month_name > 0:
    raise ValueError(f"Null calendar_month_name detected in output dataset: {null_calendar_month_name}")

if null_calendar_day_of_week > 0:
    raise ValueError(f"Null calendar_day_of_week detected in output dataset: {null_calendar_day_of_week}")

if null_calendar_day_name > 0:
    raise ValueError(f"Null calendar_day_name detected in output dataset: {null_calendar_day_name}")

if null_calendar_is_weekend > 0:
    raise ValueError(f"Null calendar_is_weekend detected in output dataset: {null_calendar_is_weekend}")

if null_calendar_is_month_start > 0:
    raise ValueError(f"Null calendar_is_month_start detected in output dataset: {null_calendar_is_month_start}")

if null_calendar_is_month_end > 0:
    raise ValueError(f"Null calendar_is_month_end detected in output dataset: {null_calendar_is_month_end}")

print("[INFO] Output validation completed successfully.")


# ========================================
# 8. WRITE DELTA TABLE
# ========================================

print("[INFO] Writing Gold dimension table to Delta format...")

(
    df_dim_calendar.write
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