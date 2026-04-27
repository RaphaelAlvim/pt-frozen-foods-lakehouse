# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_supplier
# ========================================

# Processing decisions:
# - Use CTAS (CREATE OR REPLACE TABLE AS SELECT) to create and register the Delta table in one step.
# - Use Liquid Clustering by supplier status and country to support analytical filters.
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
DATASET = "dim_supplier"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.erp_suppliers"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

GRAIN_COLUMNS = [
    "fornecedor_id"
]

CLUSTER_COLUMNS = [
    "status_fornecedor",
    "pais"
]

REQUIRED_COLUMNS = [
    "fornecedor_id",
    "nome_fornecedor",
    "pais",
    "status_fornecedor",
    "codigo_legacy",
    "ultima_sincronizacao",
    "load_date",
    "ingestion_timestamp",
    "source_file"
]

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_supplier")
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
        F.countDistinct("fornecedor_id").alias("distinct_supplier_ids"),
        F.sum(F.when(F.col("fornecedor_id").isNull(), 1).otherwise(0)).alias("null_fornecedor_id")
    )
    .collect()[0]
)

print(f"Source row count:              {source_validation['row_count']:,}")
print(f"Distinct fornecedor_id count:  {source_validation['distinct_supplier_ids']:,}")
print(f"Null fornecedor_id count:      {source_validation['null_fornecedor_id']:,}")

if source_validation["distinct_supplier_ids"] != source_validation["row_count"]:
    raise ValueError(
        f"fornecedor_id is not unique in source dataset. "
        f"Distinct: {source_validation['distinct_supplier_ids']}, Rows: {source_validation['row_count']}"
    )

if source_validation["null_fornecedor_id"] > 0:
    raise ValueError(f"Null fornecedor_id detected in source dataset: {source_validation['null_fornecedor_id']}")

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
    fornecedor_id,
    nome_fornecedor,
    pais,
    status_fornecedor,
    codigo_legacy,
    ultima_sincronizacao,
    load_date,
    ingestion_timestamp,
    source_file

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
        F.countDistinct("fornecedor_id").alias("distinct_supplier_ids"),
        F.sum(F.when(F.col("fornecedor_id").isNull(), 1).otherwise(0)).alias("null_fornecedor_id"),
        F.sum(F.when(F.col("nome_fornecedor").isNull(), 1).otherwise(0)).alias("null_nome_fornecedor"),
        F.sum(F.when(F.col("pais").isNull(), 1).otherwise(0)).alias("null_pais"),
        F.sum(F.when(F.col("status_fornecedor").isNull(), 1).otherwise(0)).alias("null_status_fornecedor"),
        F.sum(F.when(F.col("codigo_legacy").isNull(), 1).otherwise(0)).alias("null_codigo_legacy"),
        F.sum(F.when(F.col("ultima_sincronizacao").isNull(), 1).otherwise(0)).alias("null_ultima_sincronizacao"),
        F.sum(F.when(F.col("load_date").isNull(), 1).otherwise(0)).alias("null_load_date"),
        F.sum(F.when(F.col("ingestion_timestamp").isNull(), 1).otherwise(0)).alias("null_ingestion_timestamp"),
        F.sum(F.when(F.col("source_file").isNull(), 1).otherwise(0)).alias("null_source_file")
    )
    .collect()[0]
)

duplicate_rows = validation["row_count"] - validation["distinct_supplier_ids"]

print(f"Expected row count:                  {source_validation['distinct_supplier_ids']:,}")
print(f"Output row count:                    {validation['row_count']:,}")
print(f"Duplicate fornecedor_id count:       {duplicate_rows:,}")
print(f"Null fornecedor_id count:            {validation['null_fornecedor_id']:,}")
print(f"Null nome_fornecedor count:          {validation['null_nome_fornecedor']:,}")
print(f"Null pais count:                     {validation['null_pais']:,}")
print(f"Null status_fornecedor count:        {validation['null_status_fornecedor']:,}")
print(f"Null codigo_legacy count:            {validation['null_codigo_legacy']:,}")
print(f"Null ultima_sincronizacao count:     {validation['null_ultima_sincronizacao']:,}")
print(f"Null load_date count:                {validation['null_load_date']:,}")
print(f"Null ingestion_timestamp count:      {validation['null_ingestion_timestamp']:,}")
print(f"Null source_file count:              {validation['null_source_file']:,}")

if validation["row_count"] != source_validation["distinct_supplier_ids"]:
    raise ValueError(
        f"Row count mismatch detected. Expected: {source_validation['distinct_supplier_ids']}, "
        f"Got: {validation['row_count']}"
    )

if duplicate_rows > 0:
    raise ValueError(f"Duplicate fornecedor_id detected in output dataset: {duplicate_rows}")

critical_nulls = {
    "fornecedor_id": validation["null_fornecedor_id"],
    "nome_fornecedor": validation["null_nome_fornecedor"],
    "pais": validation["null_pais"],
    "status_fornecedor": validation["null_status_fornecedor"],
    "codigo_legacy": validation["null_codigo_legacy"],
    "ultima_sincronizacao": validation["null_ultima_sincronizacao"],
    "load_date": validation["null_load_date"],
    "ingestion_timestamp": validation["null_ingestion_timestamp"],
    "source_file": validation["null_source_file"]
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