# Databricks notebook source
# ========================================
# BRONZE INGESTION NOTEBOOK
# DATASET: erp_orders
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SCHEMA = "bronze"

DOMAIN = "erp"
DATASET = "erp_orders"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
RAW_CONTAINER = "raw"
BRONZE_CONTAINER = "bronze"

TABLE_NAME = f"{CATALOG}.{SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{RAW_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
BRONZE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
CHECKPOINT_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/_checkpoints/{DOMAIN}/{DATASET}/"
SCHEMA_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/_schemas/{DOMAIN}/{DATASET}/"


# ========================================
# 1. CONTEXT SETUP
# ========================================

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print("Context configured successfully")
print(f"Catalog: {spark.catalog.currentCatalog()}")
print(f"Schema: {spark.catalog.currentDatabase()}")


# ========================================
# 2. CONFIGURATION SUMMARY
# ========================================

print(" ")
print("========== CONFIGURATION SUMMARY ==========")
print(f"Table name      : {TABLE_NAME}")
print(f"Source path     : {SOURCE_PATH}")
print(f"Bronze path     : {BRONZE_PATH}")
print(f"Checkpoint path : {CHECKPOINT_PATH}")
print(f"Schema path     : {SCHEMA_PATH}")
print("===========================================")


# ========================================
# 3. PRE-CHECKS
# ========================================

raw_items = dbutils.fs.ls(SOURCE_PATH)
bronze_items = dbutils.fs.ls(f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print(" ")
print("Pre-checks completed successfully")
print(f"RAW path accessible      : yes ({len(raw_items)} items found)")
print(f"Bronze container access  : yes ({len(bronze_items)} items found)")


# ========================================
# 4. AUTO LOADER READ
# ========================================

from pyspark.sql.functions import current_timestamp, col

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_PATH)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("header", "true")
    .load(SOURCE_PATH)
)

print(" ")
print("Auto Loader stream created successfully")


# ========================================
# 5. TECHNICAL COLUMNS
# ========================================

df = (
    df.withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("source_file", col("_metadata.file_path"))
)

print("Technical columns added successfully")


# ========================================
# 6. WRITE TO BRONZE (DELTA)
# ========================================

query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .option("path", BRONZE_PATH)
    .trigger(availableNow=True)
    .start()
)

print(" ")
print("Streaming write started...")

query.awaitTermination()

print("Streaming write completed successfully")


# ========================================
# 7. REGISTER TABLE IN UNITY CATALOG
# ========================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME}
USING DELTA
LOCATION '{BRONZE_PATH}'
""")

print("Unity Catalog table registered successfully")


# ========================================
# 8. FINAL STATUS
# ========================================

print(" ")
print("===========================================")
print("BRONZE INGESTION COMPLETED SUCCESSFULLY")
print(f"Dataset         : {DATASET}")
print(f"Registered table: {TABLE_NAME}")
print(f"Delta location  : {BRONZE_PATH}")
print("===========================================")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation (Development Only)
# MAGIC
# MAGIC The following checks are used for data validation and debugging during development.
# MAGIC
# MAGIC They are controlled by the `RUN_VALIDATION` flag and should remain disabled in production execution.

# COMMAND ----------

# ========================================
# BRONZE VALIDATION CONTROL (DEV ONLY)
# ========================================

RUN_VALIDATION = False

if RUN_VALIDATION:

    TABLE_FULL_NAME = f"{CATALOG}.{SCHEMA}.{DATASET}"
    BRONZE_DATASET_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

    print(" ")
    print("===========================================")
    print("BRONZE VALIDATION STARTED")
    print(f"Dataset         : {DATASET}")
    print(f"Table           : {TABLE_FULL_NAME}")
    print(f"Storage path    : {BRONZE_DATASET_PATH}")
    print("===========================================")

    # ----------------------------------------
    # 1. PREVIEW TABLE DATA
    # ----------------------------------------

    try:
        print(" ")
        print("Step 1 - Preview table data")
        display(
            spark.sql(f"""
            SELECT *
            FROM {TABLE_FULL_NAME}
            LIMIT 10
            """)
        )
        print("Step 1 completed successfully")
    except Exception as e:
        print("Step 1 failed")
        print(f"Error: {e}")

    # ----------------------------------------
    # 2. LIST STORAGE FILES
    # ----------------------------------------

    try:
        print(" ")
        print("Step 2 - List storage files")
        files = dbutils.fs.ls(BRONZE_DATASET_PATH)
        display(files)
        print(f"Step 2 completed successfully - {len(files)} items found")
    except Exception as e:
        print("Step 2 failed")
        print(f"Error: {e}")

    # ----------------------------------------
    # 3. DESCRIBE TABLE
    # ----------------------------------------

    try:
        print(" ")
        print("Step 3 - Describe table")
        display(
            spark.sql(f"""
            DESCRIBE TABLE {TABLE_FULL_NAME}
            """)
        )
        print("Step 3 completed successfully")
    except Exception as e:
        print("Step 3 failed")
        print(f"Error: {e}")

    print(" ")
    print("===========================================")
    print("BRONZE VALIDATION FINISHED")
    print("===========================================")