# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_calendar
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "dimensions"
DATASET = "dim_calendar"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.reference_calendar"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_COLUMNS = [
    "calendar_year",
    "calendar_month"
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
    "is_fim_mes"
]

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_calendar")
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

# Validate required columns before creating the Gold dimension.
missing_columns = [c for c in REQUIRED_COLUMNS if c not in df_source.columns]

if missing_columns:
    raise ValueError(f"Missing required columns in source dataset: {missing_columns}")

# Consolidated validation to reduce unnecessary scans.
source_validation = (
    df_source
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("data").alias("distinct_dates"),
        F.sum(F.when(F.col("data").isNull(), 1).otherwise(0)).alias("null_data")
    )
    .collect()[0]
)

print(f"Source row count:       {source_validation['row_count']:,}")
print(f"Distinct dates:         {source_validation['distinct_dates']:,}")
print(f"Null data:              {source_validation['null_data']:,}")

if source_validation["null_data"] > 0:
    raise ValueError("Null data detected in source dataset.")

if source_validation["row_count"] != source_validation["distinct_dates"]:
    raise ValueError("Duplicate calendar dates detected in source dataset.")

print("[INFO] Source validation completed successfully.")

# ========================================
# 3. CREATE DIMENSION TABLE
# ========================================

print("[INFO] Creating Gold dimension table using CTAS...")

# CTAS creates, overwrites, registers, and materializes the Delta table in one step.
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
    data AS calendar_date,
    ano AS calendar_year,
    mes AS calendar_month,
    dia AS calendar_day,
    trimestre AS calendar_quarter,
    nome_mes AS calendar_month_name,
    dia_semana AS calendar_day_of_week,
    nome_dia_semana AS calendar_day_name,
    is_fim_de_semana AS calendar_is_weekend,
    is_inicio_mes AS calendar_is_month_start,
    is_fim_mes AS calendar_is_month_end

FROM {SOURCE_TABLE}
""")

print("[INFO] Gold dimension table created successfully.")

# ========================================
# 4. OPTIMIZATION
# ========================================

# OPTIMIZE is executed after the full rebuild because this dimension may support
# recurring analytical joins and time-based filters.
# In future production scenarios, this may be moved to a scheduled maintenance
# job if execution cost becomes relevant.

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

# Validate grain and critical attributes in a single aggregation.
final = (
    df_target
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("calendar_date").alias("distinct_dates"),
        F.sum(F.when(F.col("calendar_date").isNull(), 1).otherwise(0)).alias("null_calendar_date"),
        F.sum(F.when(F.col("calendar_year").isNull(), 1).otherwise(0)).alias("null_calendar_year"),
        F.sum(F.when(F.col("calendar_month").isNull(), 1).otherwise(0)).alias("null_calendar_month"),
        F.sum(F.when(F.col("calendar_day").isNull(), 1).otherwise(0)).alias("null_calendar_day"),
        F.sum(F.when(F.col("calendar_quarter").isNull(), 1).otherwise(0)).alias("null_calendar_quarter"),
        F.sum(F.when(F.col("calendar_month_name").isNull(), 1).otherwise(0)).alias("null_calendar_month_name"),
        F.sum(F.when(F.col("calendar_day_of_week").isNull(), 1).otherwise(0)).alias("null_calendar_day_of_week"),
        F.sum(F.when(F.col("calendar_day_name").isNull(), 1).otherwise(0)).alias("null_calendar_day_name"),
        F.sum(F.when(F.col("calendar_is_weekend").isNull(), 1).otherwise(0)).alias("null_calendar_is_weekend"),
        F.sum(F.when(F.col("calendar_is_month_start").isNull(), 1).otherwise(0)).alias("null_calendar_is_month_start"),
        F.sum(F.when(F.col("calendar_is_month_end").isNull(), 1).otherwise(0)).alias("null_calendar_is_month_end")
    )
    .collect()[0]
)

duplicates = final["row_count"] - final["distinct_dates"]

print(f"Expected rows:                  {source_validation['row_count']:,}")
print(f"Rows:                           {final['row_count']:,}")
print(f"Duplicates:                     {duplicates:,}")
print(f"Null calendar_date:             {final['null_calendar_date']}")
print(f"Null calendar_year:             {final['null_calendar_year']}")
print(f"Null calendar_month:            {final['null_calendar_month']}")
print(f"Null calendar_day:              {final['null_calendar_day']}")
print(f"Null calendar_quarter:          {final['null_calendar_quarter']}")
print(f"Null calendar_month_name:       {final['null_calendar_month_name']}")
print(f"Null calendar_day_of_week:      {final['null_calendar_day_of_week']}")
print(f"Null calendar_day_name:         {final['null_calendar_day_name']}")
print(f"Null calendar_is_weekend:       {final['null_calendar_is_weekend']}")
print(f"Null calendar_is_month_start:   {final['null_calendar_is_month_start']}")
print(f"Null calendar_is_month_end:     {final['null_calendar_is_month_end']}")

if final["row_count"] != source_validation["row_count"]:
    raise ValueError("Row count mismatch detected.")

if duplicates > 0:
    raise ValueError("Duplicate calendar_date detected.")

critical_nulls = {
    "calendar_date": final["null_calendar_date"],
    "calendar_year": final["null_calendar_year"],
    "calendar_month": final["null_calendar_month"],
    "calendar_day": final["null_calendar_day"],
    "calendar_quarter": final["null_calendar_quarter"],
    "calendar_month_name": final["null_calendar_month_name"],
    "calendar_day_of_week": final["null_calendar_day_of_week"],
    "calendar_day_name": final["null_calendar_day_name"],
    "calendar_is_weekend": final["null_calendar_is_weekend"],
    "calendar_is_month_start": final["null_calendar_is_month_start"],
    "calendar_is_month_end": final["null_calendar_is_month_end"]
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