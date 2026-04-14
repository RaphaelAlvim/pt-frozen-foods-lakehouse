# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_customer
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "dimensions"
DATASET = "dim_customer"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

ORDERS_CUSTOMERS_DATASET = "silver_orders_customers"

ORDERS_CUSTOMERS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{ORDERS_CUSTOMERS_DATASET}"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_KEYS = ["status_cliente", "segmento"]

AUTO_OPTIMIZE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}


# ========================================
# 1. CONTEXT SETUP
# ========================================

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_customer")
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
print(f"Orders customers table:          {ORDERS_CUSTOMERS_TABLE}")
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
spark.sql(f"DESCRIBE TABLE {ORDERS_CUSTOMERS_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")


# ========================================
# 4. READ SOURCE DATA
# ========================================

df_orders_customers = spark.table(ORDERS_CUSTOMERS_TABLE)

print("[INFO] Source data loaded successfully.")
print(f"[INFO] Orders customers row count:           {df_orders_customers.count():,}")


# ========================================
# 5. SOURCE VALIDATION
# ========================================

print("[INFO] Validating source datasets...")

null_cliente_id_source = df_orders_customers.filter(F.col("cliente_id").isNull()).count()

required_columns = [
    "cliente_id",
    "tipo_cliente",
    "segmento",
    "cliente_cidade",
    "distrito",
    "potencial_valor",
    "cluster_comercial",
    "status_cliente"
]

missing_columns = [c for c in required_columns if c not in df_orders_customers.columns]

print(f"[INFO] Null cliente_id in source:            {null_cliente_id_source:,}")
print(f"[INFO] Missing required columns:             {missing_columns}")

if null_cliente_id_source > 0:
    raise ValueError(f"Null cliente_id detected in source dataset: {null_cliente_id_source}")

if missing_columns:
    raise ValueError(f"Missing required columns in source dataset: {missing_columns}")

print("[INFO] Source validation completed successfully.")


# ========================================
# 6. BUILD DIMENSION DATASET
# ========================================

print("[INFO] Building Gold dimension dataset with explicit column selection...")

df_dim_customer = (
    df_orders_customers
    .select(
        F.col("cliente_id"),
        F.col("tipo_cliente"),
        F.col("segmento"),
        F.col("cliente_cidade"),
        F.col("distrito"),
        F.col("potencial_valor"),
        F.col("cluster_comercial"),
        F.col("status_cliente")
    )
    .dropDuplicates(["cliente_id"])
)

print("[INFO] Gold dimension dataset built successfully.")
print(f"[INFO] Row count after build:                {df_dim_customer.count():,}")


# ========================================
# 7. OUTPUT VALIDATION
# ========================================

print("[INFO] Validating Gold dimension output...")

expected_row_count = df_orders_customers.select("cliente_id").distinct().count()
final_row_count = df_dim_customer.count()

duplicate_cliente_id_count = (
    df_dim_customer
    .groupBy("cliente_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_cliente_id_final = df_dim_customer.filter(F.col("cliente_id").isNull()).count()
null_tipo_cliente_final = df_dim_customer.filter(F.col("tipo_cliente").isNull()).count()
null_segmento_final = df_dim_customer.filter(F.col("segmento").isNull()).count()
null_cliente_cidade_final = df_dim_customer.filter(F.col("cliente_cidade").isNull()).count()
null_distrito_final = df_dim_customer.filter(F.col("distrito").isNull()).count()
null_potencial_valor_final = df_dim_customer.filter(F.col("potencial_valor").isNull()).count()
null_cluster_comercial_final = df_dim_customer.filter(F.col("cluster_comercial").isNull()).count()
null_status_cliente_final = df_dim_customer.filter(F.col("status_cliente").isNull()).count()

print(f"[INFO] Expected row count:                   {expected_row_count:,}")
print(f"[INFO] Output row count:                     {final_row_count:,}")
print(f"[INFO] Duplicate cliente_id count:          {duplicate_cliente_id_count:,}")
print(f"[INFO] Null cliente_id count:               {null_cliente_id_final:,}")
print(f"[INFO] Null tipo_cliente count:             {null_tipo_cliente_final:,}")
print(f"[INFO] Null segmento count:                 {null_segmento_final:,}")
print(f"[INFO] Null cliente_cidade count:           {null_cliente_cidade_final:,}")
print(f"[INFO] Null distrito count:                 {null_distrito_final:,}")
print(f"[INFO] Null potencial_valor count:          {null_potencial_valor_final:,}")
print(f"[INFO] Null cluster_comercial count:        {null_cluster_comercial_final:,}")
print(f"[INFO] Null status_cliente count:           {null_status_cliente_final:,}")

if final_row_count != expected_row_count:
    raise ValueError(
        f"Row count mismatch detected. Expected: {expected_row_count}, Got: {final_row_count}"
    )

if duplicate_cliente_id_count > 0:
    raise ValueError(f"Duplicate cliente_id detected in output dataset: {duplicate_cliente_id_count}")

if null_cliente_id_final > 0:
    raise ValueError(f"Null cliente_id detected in output dataset: {null_cliente_id_final}")

if null_tipo_cliente_final > 0:
    raise ValueError(f"Null tipo_cliente detected in output dataset: {null_tipo_cliente_final}")

if null_segmento_final > 0:
    raise ValueError(f"Null segmento detected in output dataset: {null_segmento_final}")

if null_cliente_cidade_final > 0:
    raise ValueError(f"Null cliente_cidade detected in output dataset: {null_cliente_cidade_final}")

if null_distrito_final > 0:
    raise ValueError(f"Null distrito detected in output dataset: {null_distrito_final}")

if null_potencial_valor_final > 0:
    raise ValueError(f"Null potencial_valor detected in output dataset: {null_potencial_valor_final}")

if null_cluster_comercial_final > 0:
    raise ValueError(f"Null cluster_comercial detected in output dataset: {null_cluster_comercial_final}")

if null_status_cliente_final > 0:
    raise ValueError(f"Null status_cliente detected in output dataset: {null_status_cliente_final}")

print("[INFO] Output validation completed successfully.")


# ========================================
# 8. WRITE DELTA TABLE
# ========================================

print("[INFO] Writing Gold dimension table to Delta format...")

(
    df_dim_customer.write
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