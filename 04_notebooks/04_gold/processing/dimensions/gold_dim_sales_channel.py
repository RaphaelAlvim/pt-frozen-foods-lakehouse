# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_sales_channel
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "dimensions"
DATASET = "dim_sales_channel"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

SALES_CHANNELS_DATASET = "reference_sales_channels"

SALES_CHANNELS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{SALES_CHANNELS_DATASET}"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_KEYS = ["nome_canal"]

AUTO_OPTIMIZE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}


# ========================================
# 1. CONTEXT SETUP
# ========================================

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_sales_channel")
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
print(f"Sales channels table:            {SALES_CHANNELS_TABLE}")
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
spark.sql(f"DESCRIBE TABLE {SALES_CHANNELS_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")


# ========================================
# 4. READ SOURCE DATA
# ========================================

df_sales_channels = spark.table(SALES_CHANNELS_TABLE)

print("[INFO] Source data loaded successfully.")
print(f"[INFO] Sales channels row count:             {df_sales_channels.count():,}")


# ========================================
# 5. SOURCE VALIDATION
# ========================================

print("[INFO] Validating source datasets...")

raw_sales_channels_count = df_sales_channels.count()
distinct_canal_ids = df_sales_channels.select("canal_id").distinct().count()
null_canal_id_count = df_sales_channels.filter(F.col("canal_id").isNull()).count()

required_columns = [
    "canal_id",
    "nome_canal",
    "descricao"
]

missing_columns = [c for c in required_columns if c not in df_sales_channels.columns]

print(f"[INFO] canal_id distinct count:             {distinct_canal_ids:,}")
print(f"[INFO] Null canal_id count:                  {null_canal_id_count:,}")
print(f"[INFO] Missing required columns:             {missing_columns}")

if distinct_canal_ids != raw_sales_channels_count:
    raise ValueError(
        f"canal_id is not unique in source dataset. Distinct: {distinct_canal_ids}, Rows: {raw_sales_channels_count}"
    )

if null_canal_id_count > 0:
    raise ValueError(f"Null canal_id detected in source dataset: {null_canal_id_count}")

if missing_columns:
    raise ValueError(f"Missing required columns in source dataset: {missing_columns}")

print("[INFO] Source validation completed successfully.")


# ========================================
# 6. BUILD DIMENSION DATASET
# ========================================

print("[INFO] Building Gold dimension dataset with explicit column selection...")

df_dim_sales_channel = (
    df_sales_channels
    .select(
        F.col("canal_id"),
        F.col("nome_canal"),
        F.col("descricao")
    )
    .dropDuplicates(["canal_id"])
)

print("[INFO] Gold dimension dataset built successfully.")
print(f"[INFO] Row count after build:                {df_dim_sales_channel.count():,}")


# ========================================
# 7. OUTPUT VALIDATION
# ========================================

print("[INFO] Validating Gold dimension output...")

expected_row_count = distinct_canal_ids
final_row_count = df_dim_sales_channel.count()

duplicate_canal_id_count = (
    df_dim_sales_channel
    .groupBy("canal_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_canal_id_final = df_dim_sales_channel.filter(F.col("canal_id").isNull()).count()
null_nome_canal_final = df_dim_sales_channel.filter(F.col("nome_canal").isNull()).count()
null_descricao_final = df_dim_sales_channel.filter(F.col("descricao").isNull()).count()

print(f"[INFO] Expected row count:                   {expected_row_count:,}")
print(f"[INFO] Output row count:                     {final_row_count:,}")
print(f"[INFO] Duplicate canal_id count:            {duplicate_canal_id_count:,}")
print(f"[INFO] Null canal_id count:                  {null_canal_id_final:,}")
print(f"[INFO] Null nome_canal count:               {null_nome_canal_final:,}")
print(f"[INFO] Null descricao count:                {null_descricao_final:,}")

if final_row_count != expected_row_count:
    raise ValueError(
        f"Row count mismatch detected. Expected: {expected_row_count}, Got: {final_row_count}"
    )

if duplicate_canal_id_count > 0:
    raise ValueError(f"Duplicate canal_id detected in output dataset: {duplicate_canal_id_count}")

if null_canal_id_final > 0:
    raise ValueError(f"Null canal_id detected in output dataset: {null_canal_id_final}")

if null_nome_canal_final > 0:
    raise ValueError(f"Null nome_canal detected in output dataset: {null_nome_canal_final}")

if null_descricao_final > 0:
    raise ValueError(f"Null descricao detected in output dataset: {null_descricao_final}")

print("[INFO] Output validation completed successfully.")


# ========================================
# 8. WRITE DELTA TABLE
# ========================================

print("[INFO] Writing Gold dimension table to Delta format...")

(
    df_dim_sales_channel.write
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