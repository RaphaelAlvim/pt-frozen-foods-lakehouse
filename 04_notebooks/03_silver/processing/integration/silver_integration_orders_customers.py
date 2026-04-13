# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: silver_integration_orders_customers
# ========================================

from pyspark.sql.functions import broadcast, col, lit, row_number, when
from pyspark.sql.window import Window

print("=" * 80)
print("STARTING SILVER PROCESSING: silver_integration_orders_customers")
print("=" * 80)

# ========================================
# 0. CONFIGURATION
# Environment, Storage, and Performance Settings
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "silver"

DOMAIN = "integration"
DATASET = "silver_orders_customers"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
SILVER_CONTAINER = "silver"

ORDERS_DATASET = "erp_orders"
CLIENTS_DATASET = "crm_clients"
SEGMENTATION_DATASET = "crm_segmentation"
STATUS_DATASET = "crm_status"

ORDERS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{ORDERS_DATASET}"
CLIENTS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{CLIENTS_DATASET}"
SEGMENTATION_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{SEGMENTATION_DATASET}"
STATUS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{STATUS_DATASET}"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_KEYS = "data_pedido, cliente_id"

# Session-level performance settings
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# ========================================
# 1. CONTEXT SETUP
# Unity Catalog Configuration and Governance
# ========================================

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print("[INFO] Context setup completed successfully.")

# ========================================
# 2. CONFIGURATION SUMMARY
# Execution Parameters and Metadata Overview
# ========================================

print("=" * 80)
print("SILVER PROCESSING NOTEBOOK CONFIGURATION")
print("=" * 80)
print(f"Catalog:                         {CATALOG}")
print(f"Source schema:                   {SOURCE_SCHEMA}")
print(f"Target schema:                   {TARGET_SCHEMA}")
print(f"Domain:                          {DOMAIN}")
print(f"Dataset:                         {DATASET}")
print(f"Orders table:                    {ORDERS_TABLE}")
print(f"Clients table:                   {CLIENTS_TABLE}")
print(f"Segmentation table:              {SEGMENTATION_TABLE}")
print(f"Status table:                    {STATUS_TABLE}")
print(f"Target table:                    {TARGET_TABLE}")
print(f"Target path:                     {TARGET_PATH}")
print(f"Cluster keys:                    {CLUSTER_KEYS}")
print(f"Optimize write enabled:          {spark.conf.get('spark.databricks.delta.optimizeWrite.enabled')}")
print(f"Auto compact enabled:            {spark.conf.get('spark.databricks.delta.autoCompact.enabled')}")
print("=" * 80)

# ========================================
# 3. PRE-CHECKS
# Data Availability and Environment Validation
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {ORDERS_TABLE}")
spark.sql(f"DESCRIBE TABLE {CLIENTS_TABLE}")
spark.sql(f"DESCRIBE TABLE {SEGMENTATION_TABLE}")
spark.sql(f"DESCRIBE TABLE {STATUS_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")

# ========================================
# 4. READ SOURCE DATA
# Optimized Data Loading from Silver Layer
# ========================================

df_orders = spark.table(ORDERS_TABLE)
df_clients = spark.table(CLIENTS_TABLE)
df_segmentation = spark.table(SEGMENTATION_TABLE)
df_status = spark.table(STATUS_TABLE)

orders_count_raw = df_orders.count()

print("[INFO] Source data loaded successfully.")
print(f"[INFO] Raw orders row count: {orders_count_raw}")

# ========================================
# 5. DEDUPLICATE ORDERS HEADER
# Logical Optimization: Deterministic Grain Enforcement
# ========================================

df_orders_scored = (
    df_orders
    .withColumn(
        "business_completeness_score",
        (
            when(col("data_pedido").isNotNull(), lit(1)).otherwise(lit(0)) +
            when(col("cliente_id").isNotNull(), lit(1)).otherwise(lit(0)) +
            when(col("canal_id").isNotNull(), lit(1)).otherwise(lit(0)) +
            when(col("vendedor_id").isNotNull(), lit(1)).otherwise(lit(0)) +
            when(col("cidade_entrega").isNotNull(), lit(1)).otherwise(lit(0)) +
            when(col("estado_pedido").isNotNull(), lit(1)).otherwise(lit(0)) +
            when(col("prazo_entrega_dias").isNotNull(), lit(1)).otherwise(lit(0)) +
            when(col("observacao_pedido").isNotNull(), lit(1)).otherwise(lit(0)) +
            when(col("sistema_origem").isNotNull(), lit(1)).otherwise(lit(0))
        )
    )
)

orders_window = Window.partitionBy("pedido_id").orderBy(
    col("business_completeness_score").desc(),
    col("ingestion_timestamp").desc(),
    col("source_file").desc(),
    col("usuario_ultima_alteracao").desc()
)

df_orders_dedup = (
    df_orders_scored
    .withColumn("row_num", row_number().over(orders_window))
    .filter(col("row_num") == 1)
    .drop("business_completeness_score", "row_num")
)

orders_count_before_dedup = df_orders.count()
orders_count_after_dedup = df_orders_dedup.count()
duplicate_orders_removed = orders_count_before_dedup - orders_count_after_dedup

print("[INFO] Orders header deduplicated successfully.")
print(f"[INFO] Orders count before deduplication: {orders_count_before_dedup}")
print(f"[INFO] Orders count after deduplication:  {orders_count_after_dedup}")
print(f"[INFO] Duplicate orders removed:          {duplicate_orders_removed}")

# ========================================
# 6. SELECT AND PREPARE COLUMNS
# Logical Optimization: Column Pruning and Metadata Standardization
# ========================================

df_orders_sel = df_orders_dedup.select(
    col("pedido_id"),
    col("cliente_id"),
    col("data_pedido"),
    col("canal_id"),
    col("vendedor_id"),
    col("cidade_entrega"),
    col("estado_pedido"),
    col("prazo_entrega_dias"),
    col("observacao_pedido"),
    col("sistema_origem"),
    col("usuario_ultima_alteracao"),
    col("load_date").alias("order_load_date"),
    col("ingestion_timestamp").alias("order_ingestion_timestamp"),
    col("source_file").alias("order_source_file")
)

df_clients_sel = df_clients.select(
    col("cliente_id"),
    col("nome_cliente"),
    col("tipo_cliente"),
    col("nif"),
    col("data_registo"),
    col("cidade").alias("cliente_cidade"),
    col("distrito"),
    col("canal_captacao"),
    col("score_atividade"),
    col("obs_comercial")
)

df_segmentation_sel = df_segmentation.select(
    col("cliente_id"),
    col("segmento"),
    col("potencial_valor"),
    col("cluster_comercial")
)

df_status_sel = df_status.select(
    col("cliente_id"),
    col("status_cliente"),
    col("data_status"),
    col("motivo_status")
)

print("[INFO] Source columns selected and standardized successfully.")

# ========================================
# 7. JOIN AND BUILD INTEGRATED DATASET
# Logical Optimization: Broadcast Joins and Join Order Control
# ========================================

df_final = (
    df_orders_sel.alias("o")
    .join(
        broadcast(df_clients_sel).alias("c"),
        on="cliente_id",
        how="left"
    )
    .join(
        broadcast(df_segmentation_sel).alias("s"),
        on="cliente_id",
        how="left"
    )
    .join(
        broadcast(df_status_sel).alias("st"),
        on="cliente_id",
        how="left"
    )
)

print("[INFO] Integrated dataset built successfully.")

# ========================================
# 8. VALIDATE OUTPUT
# Data Quality Assurance and Grain Validation
# ========================================

expected_orders_count = df_orders_dedup.count()
final_count = df_final.count()

if final_count != expected_orders_count:
    raise ValueError(
        f"Row count mismatch detected. Expected: {expected_orders_count}, Found: {final_count}"
    )

duplicate_orders = (
    df_final.groupBy("pedido_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicate_orders > 0:
    raise ValueError(f"Duplicate pedido_id detected in output dataset: {duplicate_orders}")

null_cliente_id = df_final.filter(col("cliente_id").isNull()).count()
if null_cliente_id > 0:
    raise ValueError(f"Null cliente_id detected in output dataset: {null_cliente_id}")

null_pedido_id = df_final.filter(col("pedido_id").isNull()).count()
if null_pedido_id > 0:
    raise ValueError(f"Null pedido_id detected in output dataset: {null_pedido_id}")

print("[INFO] Output validation completed successfully.")
print(f"[INFO] Expected row count after deduplication: {expected_orders_count}")
print(f"[INFO] Output row count:                        {final_count}")
print(f"[INFO] Duplicate pedido_id count:               {duplicate_orders}")
print(f"[INFO] Null cliente_id count:                   {null_cliente_id}")
print(f"[INFO] Null pedido_id count:                    {null_pedido_id}")

# ========================================
# 9. WRITE DELTA TABLE
# Physical Optimization: External Delta Table
# ========================================

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", TARGET_PATH)
    .saveAsTable(TARGET_TABLE)
)

print("[INFO] Delta table written successfully.")

# ========================================
# 10. APPLY TABLE PERFORMANCE SETTINGS
# Physical Optimization: Auto Optimize
# ========================================

spark.sql(f"""
ALTER TABLE {TARGET_TABLE}
SET TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
""")

print("[INFO] Auto Optimize table properties applied successfully.")

# ========================================
# 11. APPLY LIQUID CLUSTERING
# Physical Optimization: Modern Delta Lake Layout Strategy
# ========================================

spark.sql(f"""
ALTER TABLE {TARGET_TABLE}
CLUSTER BY ({CLUSTER_KEYS})
""")

print("[INFO] Liquid clustering applied successfully.")

# ========================================
# 12. FINAL STATUS
# Operational Validation and Table Inspection
# ========================================

print("=" * 80)
print("FINAL TABLE DETAIL")
print("=" * 80)

final_detail = spark.sql(f"DESCRIBE DETAIL {TARGET_TABLE}").collect()[0]

print(f"Format:              {final_detail['format']}")
print(f"Table name:          {final_detail['name']}")
print(f"Location:            {final_detail['location']}")
print(f"Created at:          {final_detail['createdAt']}")
print(f"Last modified:       {final_detail['lastModified']}")
print(f"Partition columns:   {final_detail['partitionColumns']}")
print(f"Clustering columns:  {final_detail['clusteringColumns']}")
print(f"Number of files:     {final_detail['numFiles']}")
print(f"Size in bytes:       {final_detail['sizeInBytes']}")

print("=" * 80)
print("SILVER PROCESSING COMPLETED SUCCESSFULLY")
print("=" * 80)
print(f"Target table: {TARGET_TABLE}")
print(f"Target path:  {TARGET_PATH}")