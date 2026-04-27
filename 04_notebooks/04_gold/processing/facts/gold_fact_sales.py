# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: fact_sales
# ========================================

# Processing decisions:
# - Use CTAS (CREATE OR REPLACE TABLE AS SELECT) to create and register the Delta table in one step.
# - Use Liquid Clustering to optimize recurring queries by date and product.
# - Avoid unnecessary cache, repartition, or coalesce operations.
# - Consolidate validations to reduce unnecessary scans.
# - Keep critical source and output validations to protect data quality.

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "facts"
DATASET = "fact_sales"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

ORDER_ITEMS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.erp_order_items"
ORDERS_CUSTOMERS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.silver_orders_customers"
PRODUCTS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.erp_products"
SALES_CHANNELS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.reference_sales_channels"
CALENDAR_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.reference_calendar"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_COLUMNS = [
    "data_pedido",
    "produto_id"
]

print("=" * 80)
print("STARTING GOLD PROCESSING: fact_sales")
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
print(f"Order items table:               {ORDER_ITEMS_TABLE}")
print(f"Orders customers table:          {ORDERS_CUSTOMERS_TABLE}")
print(f"Products table:                  {PRODUCTS_TABLE}")
print(f"Sales channels table:            {SALES_CHANNELS_TABLE}")
print(f"Calendar table:                  {CALENDAR_TABLE}")
print(f"Target table:                    {TARGET_TABLE}")
print(f"Target path:                     {TARGET_PATH}")
print(f"Cluster columns:                 {', '.join(CLUSTER_COLUMNS)}")
print(f"Optimization strategy:           Delta Auto Optimize + Liquid Clustering")
print(f"Partitioning strategy:           None")
print("=" * 80)

# ========================================
# 2. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")

spark.sql(f"DESCRIBE TABLE {ORDER_ITEMS_TABLE}")
spark.sql(f"DESCRIBE TABLE {ORDERS_CUSTOMERS_TABLE}")
spark.sql(f"DESCRIBE TABLE {PRODUCTS_TABLE}")
spark.sql(f"DESCRIBE TABLE {SALES_CHANNELS_TABLE}")
spark.sql(f"DESCRIBE TABLE {CALENDAR_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")

# ========================================
# 3. SOURCE VALIDATION
# ========================================

print("[INFO] Validating source primary key and critical business keys...")

source_validation = (
    spark.table(ORDER_ITEMS_TABLE)
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("item_pedido_id").alias("distinct_item_ids"),
        F.sum(F.when(F.col("item_pedido_id").isNull(), 1).otherwise(0)).alias("null_item_pedido_id"),
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_produto_id"),
        F.sum(F.when(F.col("produto_id").isNotNull(), 1).otherwise(0)).alias("valid_order_items")
    )
    .collect()[0]
)

raw_order_items_count = source_validation["row_count"]
valid_order_items_count = source_validation["valid_order_items"]
excluded_order_items_count = raw_order_items_count - valid_order_items_count
excluded_pct = round((excluded_order_items_count / raw_order_items_count) * 100, 4) if raw_order_items_count else 0

print(f"Raw order items count:            {raw_order_items_count:,}")
print(f"Distinct item_pedido_id count:    {source_validation['distinct_item_ids']:,}")
print(f"Null item_pedido_id count:        {source_validation['null_item_pedido_id']:,}")
print(f"Null produto_id count:            {source_validation['null_produto_id']:,}")
print(f"Valid order items count:          {valid_order_items_count:,}")
print(f"Rows excluded:                    {excluded_order_items_count:,}")
print(f"Excluded percentage:              {excluded_pct}%")

if source_validation["distinct_item_ids"] != raw_order_items_count:
    raise ValueError(
        f"item_pedido_id is not unique. Distinct: {source_validation['distinct_item_ids']}, Rows: {raw_order_items_count}"
    )

if source_validation["null_item_pedido_id"] > 0:
    raise ValueError(f"Null item_pedido_id detected in source dataset: {source_validation['null_item_pedido_id']}")

print("[INFO] Source validation completed successfully.")

# ========================================
# 4. CREATE FACT TABLE
# ========================================

print("[INFO] Creating Gold fact table using CTAS...")

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
WITH orders_customers_clean AS (
    SELECT
        *,
        COALESCE(vendedor_id, 'UNKNOWN') AS vendedor_id_clean
    FROM {ORDERS_CUSTOMERS_TABLE}
),

fact_base AS (
    SELECT
        oi.item_pedido_id,
        oi.pedido_id,
        oi.produto_id,
        oi.quantidade,
        oi.preco_lista_unitario,
        oi.desconto_unitario,
        oi.preco_venda_unitario,
        oi.custo_unitario,
        oi.flag_promocao,
        oi.lote_fornecedor,
        oi.load_date,
        oi.ingestion_timestamp,
        oi.source_file,

        oc.cliente_id,
        oc.canal_id,
        oc.data_pedido,
        oc.vendedor_id_clean,
        oc.cidade_entrega,
        oc.estado_pedido,
        oc.prazo_entrega_dias,
        oc.tipo_cliente,
        oc.cliente_cidade,
        oc.distrito,
        oc.segmento,
        oc.potencial_valor,
        oc.cluster_comercial,
        oc.status_cliente,
        oc.order_load_date,
        oc.order_ingestion_timestamp,
        oc.order_source_file

    FROM {ORDER_ITEMS_TABLE} oi
    INNER JOIN orders_customers_clean oc
        ON oi.pedido_id = oc.pedido_id
    WHERE oi.produto_id IS NOT NULL
),

fact_enriched AS (
    SELECT
        fb.item_pedido_id,
        fb.pedido_id,
        fb.produto_id,
        fb.cliente_id,
        fb.canal_id,
        p.fornecedor_id,

        fb.data_pedido,
        cal.ano AS calendar_year,
        cal.trimestre AS calendar_quarter,
        cal.mes AS calendar_month,
        cal.dia AS calendar_day,
        cal.nome_mes AS calendar_month_name,
        cal.dia_semana AS calendar_day_of_week,
        cal.nome_dia_semana AS calendar_day_of_week_name,
        cal.is_fim_de_semana AS calendar_is_weekend,
        cal.is_inicio_mes AS calendar_is_month_start,
        cal.is_fim_mes AS calendar_is_month_end,

        fb.vendedor_id_clean AS vendedor_id,
        fb.cidade_entrega,
        fb.estado_pedido,
        fb.prazo_entrega_dias,
        fb.tipo_cliente,
        fb.cliente_cidade,
        fb.distrito,
        fb.segmento,
        fb.potencial_valor,
        fb.cluster_comercial,
        fb.status_cliente,

        p.produto_nome,
        p.categoria,
        p.marca,
        p.status_produto,
        sc.nome_canal,
        sc.descricao AS descricao_canal,

        fb.quantidade AS quantity_sold,
        fb.preco_lista_unitario,
        fb.desconto_unitario,
        fb.preco_venda_unitario,
        fb.custo_unitario,
        fb.quantidade * fb.preco_lista_unitario AS gross_sales_amount,
        fb.quantidade * fb.preco_venda_unitario AS net_sales_amount,
        fb.quantidade * fb.custo_unitario AS total_cost_amount,
        (fb.quantidade * fb.preco_venda_unitario) - (fb.quantidade * fb.custo_unitario) AS gross_margin_amount,
        1 AS line_count,

        fb.flag_promocao,
        fb.lote_fornecedor,

        fb.load_date AS item_load_date,
        fb.ingestion_timestamp AS item_ingestion_timestamp,
        fb.source_file AS item_source_file,
        fb.order_load_date,
        fb.order_ingestion_timestamp,
        fb.order_source_file

    FROM fact_base fb
    LEFT JOIN {PRODUCTS_TABLE} p
        ON fb.produto_id = p.produto_id
    LEFT JOIN {SALES_CHANNELS_TABLE} sc
        ON fb.canal_id = sc.canal_id
    LEFT JOIN {CALENDAR_TABLE} cal
        ON fb.data_pedido = cal.data
)

SELECT
    *,
    SUM(net_sales_amount) OVER (PARTITION BY pedido_id) AS average_order_value

FROM fact_enriched
""")

print("[INFO] Gold fact table created successfully.")

# ========================================
# 5. OPTIMIZATION
# ========================================

# OPTIMIZE is executed after the full rebuild because this fact table is intended
# for recurring analytical consumption and downstream Gold/Analytics processing.
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

target_validation = (
    df_target
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("item_pedido_id").alias("distinct_item_ids"),
        F.sum(F.when(F.col("item_pedido_id").isNull(), 1).otherwise(0)).alias("null_item_pedido_id"),
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_produto_id"),
        F.sum(F.when(F.col("produto_nome").isNull(), 1).otherwise(0)).alias("orphan_product_rows"),
        F.sum(F.when(F.col("nome_canal").isNull(), 1).otherwise(0)).alias("orphan_channel_rows"),
        F.sum(F.when(F.col("calendar_year").isNull(), 1).otherwise(0)).alias("orphan_calendar_rows")
    )
    .collect()[0]
)

duplicate_item_ids = target_validation["row_count"] - target_validation["distinct_item_ids"]

print(f"Expected row count:              {valid_order_items_count:,}")
print(f"Output row count:                {target_validation['row_count']:,}")
print(f"Duplicate item_pedido_id count:  {duplicate_item_ids:,}")
print(f"Null item_pedido_id count:       {target_validation['null_item_pedido_id']:,}")
print(f"Null produto_id count:           {target_validation['null_produto_id']:,}")
print(f"Orphan product rows:             {target_validation['orphan_product_rows']:,}")
print(f"Orphan sales channel rows:       {target_validation['orphan_channel_rows']:,}")
print(f"Orphan calendar rows:            {target_validation['orphan_calendar_rows']:,}")

if target_validation["row_count"] != valid_order_items_count:
    raise ValueError(
        f"Row count mismatch detected. Expected: {valid_order_items_count}, Got: {target_validation['row_count']}"
    )

if duplicate_item_ids > 0:
    raise ValueError(f"Duplicate item_pedido_id detected in output dataset: {duplicate_item_ids}")

if target_validation["null_item_pedido_id"] > 0:
    raise ValueError(f"Null item_pedido_id detected in output dataset: {target_validation['null_item_pedido_id']}")

if target_validation["null_produto_id"] > 0:
    raise ValueError(f"Null produto_id detected in output dataset: {target_validation['null_produto_id']}")

if target_validation["orphan_product_rows"] > 0:
    raise ValueError(f"Orphan product rows detected: {target_validation['orphan_product_rows']}")

if target_validation["orphan_channel_rows"] > 0:
    raise ValueError(f"Orphan sales channel rows detected: {target_validation['orphan_channel_rows']}")

if target_validation["orphan_calendar_rows"] > 0:
    raise ValueError(f"Orphan calendar rows detected: {target_validation['orphan_calendar_rows']}")

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