# Databricks notebook source
# ========================================
# BRONZE INGESTION NOTEBOOK
# DATASET: erp_salespersons
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SCHEMA = "bronze"

DOMAIN = "erp"
DATASET = "erp_salespersons"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
RAW_CONTAINER = "raw"
BRONZE_CONTAINER = "bronze"

TABLE_NAME = f"{CATALOG}.{SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
BRONZE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
CHECKPOINT_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/_checkpoints/{DOMAIN}/{DATASET}/"
SCHEMA_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/_schemas/{DOMAIN}/{DATASET}/"

print("=" * 80)
print("STARTING BRONZE INGESTION: erp_salespersons")
print("=" * 80)

# ========================================
# 1. CONTEXT SETUP
# ========================================

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print("[INFO] Context setup completed successfully.")

# ========================================
# 2. CONFIGURATION SUMMARY
# ========================================

print("=" * 80)
print("BRONZE INGESTION CONFIGURATION")
print("=" * 80)
print(f"Table name:       {TABLE_NAME}")
print(f"Source path:      {SOURCE_PATH}")
print(f"Bronze path:      {BRONZE_PATH}")
print(f"Checkpoint path:  {CHECKPOINT_PATH}")
print(f"Schema path:      {SCHEMA_PATH}")
print("=" * 80)

# ========================================
# 3. PRE-CHECKS
# ========================================

print("[INFO] Checking RAW source path access...")
raw_items = dbutils.fs.ls(SOURCE_PATH)

if len(raw_items) == 0:
    raise ValueError(f"No files found in source path: {SOURCE_PATH}")

print("[INFO] Checking Bronze container access...")
dbutils.fs.ls(f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print(f"[INFO] RAW path accessible: {len(raw_items)} items found.")
print("[INFO] Bronze container access validated.")
print("[INFO] Pre-checks completed successfully.")

# ========================================
# 4. AUTO LOADER READ
# ========================================

print("[INFO] Creating Auto Loader stream...")

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(SOURCE_PATH)
)

print("[INFO] Auto Loader stream created successfully.")

# ========================================
# 5. TECHNICAL COLUMNS
# ========================================

print("[INFO] Adding technical metadata columns...")

df = (
    df
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.col("_metadata.file_path"))
)

print("[INFO] Technical columns added successfully.")

# ========================================
# 6. WRITE TO BRONZE
# ========================================

print("[INFO] Starting Bronze streaming write...")

query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .option("path", BRONZE_PATH)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("[INFO] Bronze streaming write completed successfully.")

# ========================================
# 7. REGISTER TABLE
# ========================================

print("[INFO] Registering Bronze table in Unity Catalog...")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME}
USING DELTA
LOCATION '{BRONZE_PATH}'
""")

print("[INFO] Unity Catalog table registered successfully.")

# ========================================
# 8. FINAL STATUS
# ========================================

detail = spark.sql(f"DESCRIBE DETAIL {TABLE_NAME}").collect()[0].asDict()

print("=" * 80)
print("FINAL TABLE DETAIL")
print("=" * 80)
print(f"Files: {detail.get('numFiles')}")
print(f"Size:  {detail.get('sizeInBytes')}")

print("=" * 80)
print("COMPLETED")
print("=" * 80)