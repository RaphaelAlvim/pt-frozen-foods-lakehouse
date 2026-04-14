# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_supplier
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "dimensions"
DATASET = "dim_supplier"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

SUPPLIERS_DATASET = "erp_suppliers"

SUPPLIERS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{SUPPLIERS_DATASET}"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_KEYS = ["status_fornecedor", "pais"]

AUTO_OPTIMIZE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}


# ========================================
# 1. CONTEXT SETUP
# ========================================

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_supplier")
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
print(f"Suppliers table:                 {SUPPLIERS_TABLE}")
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
spark.sql(f"DESCRIBE TABLE {SUPPLIERS_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")


# ========================================
# 4. READ SOURCE DATA
# ========================================

df_suppliers = spark.table(SUPPLIERS_TABLE)

print("[INFO] Source data loaded successfully.")
print(f"[INFO] Suppliers row count:                  {df_suppliers.count():,}")


# ========================================
# 5. SOURCE VALIDATION
# ========================================

print("[INFO] Validating source datasets...")

raw_supplier_count = df_suppliers.count()
distinct_supplier_ids = df_suppliers.select("fornecedor_id").distinct().count()
null_supplier_id_count = df_suppliers.filter(F.col("fornecedor_id").isNull()).count()

required_columns = [
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

missing_columns = [c for c in required_columns if c not in df_suppliers.columns]

print(f"[INFO] fornecedor_id distinct count:         {distinct_supplier_ids:,}")
print(f"[INFO] Null fornecedor_id count:             {null_supplier_id_count:,}")
print(f"[INFO] Missing required columns:             {missing_columns}")

if distinct_supplier_ids != raw_supplier_count:
    raise ValueError(
        f"fornecedor_id is not unique in source dataset. Distinct: {distinct_supplier_ids}, Rows: {raw_supplier_count}"
    )

if null_supplier_id_count > 0:
    raise ValueError(f"Null fornecedor_id detected in source dataset: {null_supplier_id_count}")

if missing_columns:
    raise ValueError(f"Missing required columns in source dataset: {missing_columns}")

print("[INFO] Source validation completed successfully.")


# ========================================
# 6. BUILD DIMENSION DATASET
# ========================================

print("[INFO] Building Gold dimension dataset with explicit column selection...")

df_dim_supplier = (
    df_suppliers
    .select(
        F.col("fornecedor_id"),
        F.col("nome_fornecedor"),
        F.col("pais"),
        F.col("status_fornecedor"),
        F.col("codigo_legacy"),
        F.col("ultima_sincronizacao"),
        F.col("load_date"),
        F.col("ingestion_timestamp"),
        F.col("source_file")
    )
    .dropDuplicates(["fornecedor_id"])
)

print("[INFO] Gold dimension dataset built successfully.")
print(f"[INFO] Row count after build:                {df_dim_supplier.count():,}")


# ========================================
# 7. OUTPUT VALIDATION
# ========================================

print("[INFO] Validating Gold dimension output...")

expected_row_count = distinct_supplier_ids
final_row_count = df_dim_supplier.count()

duplicate_supplier_id_count = (
    df_dim_supplier
    .groupBy("fornecedor_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_fornecedor_id_final = df_dim_supplier.filter(F.col("fornecedor_id").isNull()).count()
null_nome_fornecedor_final = df_dim_supplier.filter(F.col("nome_fornecedor").isNull()).count()
null_pais_final = df_dim_supplier.filter(F.col("pais").isNull()).count()
null_status_fornecedor_final = df_dim_supplier.filter(F.col("status_fornecedor").isNull()).count()
null_codigo_legacy_final = df_dim_supplier.filter(F.col("codigo_legacy").isNull()).count()
null_ultima_sincronizacao_final = df_dim_supplier.filter(F.col("ultima_sincronizacao").isNull()).count()
null_load_date_final = df_dim_supplier.filter(F.col("load_date").isNull()).count()
null_ingestion_timestamp_final = df_dim_supplier.filter(F.col("ingestion_timestamp").isNull()).count()
null_source_file_final = df_dim_supplier.filter(F.col("source_file").isNull()).count()

print(f"[INFO] Expected row count:                   {expected_row_count:,}")
print(f"[INFO] Output row count:                     {final_row_count:,}")
print(f"[INFO] Duplicate fornecedor_id count:       {duplicate_supplier_id_count:,}")
print(f"[INFO] Null fornecedor_id count:            {null_fornecedor_id_final:,}")
print(f"[INFO] Null nome_fornecedor count:          {null_nome_fornecedor_final:,}")
print(f"[INFO] Null pais count:                     {null_pais_final:,}")
print(f"[INFO] Null status_fornecedor count:        {null_status_fornecedor_final:,}")
print(f"[INFO] Null codigo_legacy count:            {null_codigo_legacy_final:,}")
print(f"[INFO] Null ultima_sincronizacao count:     {null_ultima_sincronizacao_final:,}")
print(f"[INFO] Null load_date count:                {null_load_date_final:,}")
print(f"[INFO] Null ingestion_timestamp count:      {null_ingestion_timestamp_final:,}")
print(f"[INFO] Null source_file count:              {null_source_file_final:,}")

if final_row_count != expected_row_count:
    raise ValueError(
        f"Row count mismatch detected. Expected: {expected_row_count}, Got: {final_row_count}"
    )

if duplicate_supplier_id_count > 0:
    raise ValueError(f"Duplicate fornecedor_id detected in output dataset: {duplicate_supplier_id_count}")

if null_fornecedor_id_final > 0:
    raise ValueError(f"Null fornecedor_id detected in output dataset: {null_fornecedor_id_final}")

if null_nome_fornecedor_final > 0:
    raise ValueError(f"Null nome_fornecedor detected in output dataset: {null_nome_fornecedor_final}")

if null_pais_final > 0:
    raise ValueError(f"Null pais detected in output dataset: {null_pais_final}")

if null_status_fornecedor_final > 0:
    raise ValueError(f"Null status_fornecedor detected in output dataset: {null_status_fornecedor_final}")

if null_codigo_legacy_final > 0:
    raise ValueError(f"Null codigo_legacy detected in output dataset: {null_codigo_legacy_final}")

if null_ultima_sincronizacao_final > 0:
    raise ValueError(f"Null ultima_sincronizacao detected in output dataset: {null_ultima_sincronizacao_final}")

if null_load_date_final > 0:
    raise ValueError(f"Null load_date detected in output dataset: {null_load_date_final}")

if null_ingestion_timestamp_final > 0:
    raise ValueError(f"Null ingestion_timestamp detected in output dataset: {null_ingestion_timestamp_final}")

if null_source_file_final > 0:
    raise ValueError(f"Null source_file detected in output dataset: {null_source_file_final}")

print("[INFO] Output validation completed successfully.")


# ========================================
# 8. WRITE DELTA TABLE
# ========================================

print("[INFO] Writing Gold dimension table to Delta format...")

(
    df_dim_supplier.write
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