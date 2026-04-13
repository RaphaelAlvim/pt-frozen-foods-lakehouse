# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: fact_sales
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "facts"
DATASET = "fact_sales"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

ORDER_ITEMS_DATASET = "erp_order_items"
ORDERS_CUSTOMERS_DATASET = "silver_orders_customers"
PRODUCTS_DATASET = "erp_products"
SALES_CHANNELS_DATASET = "reference_sales_channels"
CALENDAR_DATASET = "reference_calendar"

ORDER_ITEMS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{ORDER_ITEMS_DATASET}"
ORDERS_CUSTOMERS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{ORDERS_CUSTOMERS_DATASET}"
PRODUCTS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{PRODUCTS_DATASET}"
SALES_CHANNELS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{SALES_CHANNELS_DATASET}"
CALENDAR_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{CALENDAR_DATASET}"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_KEYS = ["data_pedido", "produto_id"]

# Auto Optimize table properties
AUTO_OPTIMIZE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}


# ========================================
# 1. CONTEXT SETUP
# ========================================

print("=" * 80)
print("STARTING GOLD PROCESSING: fact_sales")
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
print(f"Order items table:               {ORDER_ITEMS_TABLE}")
print(f"Orders customers table:          {ORDERS_CUSTOMERS_TABLE}")
print(f"Products table:                  {PRODUCTS_TABLE}")
print(f"Sales channels table:            {SALES_CHANNELS_TABLE}")
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
spark.sql(f"DESCRIBE TABLE {ORDER_ITEMS_TABLE}")
spark.sql(f"DESCRIBE TABLE {ORDERS_CUSTOMERS_TABLE}")
spark.sql(f"DESCRIBE TABLE {PRODUCTS_TABLE}")
spark.sql(f"DESCRIBE TABLE {SALES_CHANNELS_TABLE}")
spark.sql(f"DESCRIBE TABLE {CALENDAR_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")


# ========================================
# 4. READ SOURCE DATA
# ========================================

df_order_items = spark.table(ORDER_ITEMS_TABLE)
df_orders_customers = spark.table(ORDERS_CUSTOMERS_TABLE)
df_products = spark.table(PRODUCTS_TABLE)
df_sales_channels = spark.table(SALES_CHANNELS_TABLE)
df_calendar = spark.table(CALENDAR_TABLE)

print("[INFO] Source data loaded successfully.")
print(f"[INFO] Raw order items row count:            {df_order_items.count():,}")
print(f"[INFO] Orders customers row count:           {df_orders_customers.count():,}")
print(f"[INFO] Products row count:                   {df_products.count():,}")
print(f"[INFO] Sales channels row count:             {df_sales_channels.count():,}")
print(f"[INFO] Calendar row count:                   {df_calendar.count():,}")


# ========================================
# 5. SOURCE VALIDATION
# ========================================

print("[INFO] Validating primary key and critical business keys...")

raw_order_items_count = df_order_items.count()
distinct_item_ids = df_order_items.select("item_pedido_id").distinct().count()
null_item_ids = df_order_items.filter(F.col("item_pedido_id").isNull()).count()
null_product_ids = df_order_items.filter(F.col("produto_id").isNull()).count()

if distinct_item_ids != raw_order_items_count:
    raise ValueError(
        f"item_pedido_id is not unique. Distinct: {distinct_item_ids}, Rows: {raw_order_items_count}"
    )

if null_item_ids > 0:
    raise ValueError(f"Null item_pedido_id detected in source dataset: {null_item_ids}")

print(f"[INFO] item_pedido_id uniqueness validated:  {distinct_item_ids:,}")
print(f"[INFO] Null item_pedido_id count:           {null_item_ids:,}")
print(f"[INFO] Null produto_id count:               {null_product_ids:,}")
print("[INFO] Source validation completed successfully.")


# ========================================
# 6. FILTER FACT CANDIDATES
# ========================================

print("[INFO] Filtering out records with null produto_id...")
print("[INFO] This rule was validated in the Gold exploratory notebook.")

df_order_items_valid = df_order_items.filter(F.col("produto_id").isNotNull())

valid_order_items_count = df_order_items_valid.count()
excluded_order_items_count = raw_order_items_count - valid_order_items_count
excluded_pct = round((excluded_order_items_count / raw_order_items_count) * 100, 4) if raw_order_items_count else 0

print(f"[INFO] Order items before filter:            {raw_order_items_count:,}")
print(f"[INFO] Order items after filter:             {valid_order_items_count:,}")
print(f"[INFO] Rows excluded:                        {excluded_order_items_count:,}")
print(f"[INFO] Excluded percentage:                  {excluded_pct}%")

# Optional standardization for nullable operational key
df_orders_customers_clean = df_orders_customers.fillna({"vendedor_id": "UNKNOWN"})

print("[INFO] Fact candidate filtering completed successfully.")


# ========================================
# 7. BUILD FACT DATASET
# ========================================

print("[INFO] Building Gold fact dataset with explicit column selection...")
print("[INFO] Applying join order control from transactional base to small dimensions...")

# Base join: order items + orders/customers
df_fact_base = (
    df_order_items_valid.alias("oi")
    .join(
        df_orders_customers_clean.alias("oc"),
        on="pedido_id",
        how="inner"
    )
)

base_join_count = df_fact_base.count()

# Product dimension
df_fact_products = (
    df_fact_base.alias("f")
    .join(
        F.broadcast(
            df_products.select(
                "produto_id",
                "produto_nome",
                "categoria",
                "marca",
                "fornecedor_id",
                "status_produto"
            )
        ).alias("p"),
        on="produto_id",
        how="left"
    )
)

# Sales channel dimension
df_fact_channels = (
    df_fact_products.alias("f")
    .join(
        F.broadcast(
            df_sales_channels.select(
                "canal_id",
                "nome_canal",
                "descricao"
            )
        ).alias("sc"),
        on="canal_id",
        how="left"
    )
)

# Calendar dimension
df_fact_joined = (
    df_fact_channels.alias("f")
    .join(
        F.broadcast(
            df_calendar.select(
                F.col("data").alias("calendar_date"),
                F.col("ano").alias("calendar_year"),
                F.col("mes").alias("calendar_month"),
                F.col("dia").alias("calendar_day"),
                F.col("trimestre").alias("calendar_quarter"),
                F.col("nome_mes").alias("calendar_month_name"),
                F.col("dia_semana").alias("calendar_day_of_week"),
                F.col("nome_dia_semana").alias("calendar_day_of_week_name"),
                F.col("is_fim_de_semana").alias("calendar_is_weekend"),
                F.col("is_inicio_mes").alias("calendar_is_month_start"),
                F.col("is_fim_mes").alias("calendar_is_month_end")
            )
        ).alias("cal"),
        F.col("f.data_pedido") == F.col("cal.calendar_date"),
        how="left"
    )
)

# Final explicit projection
df_fact_sales = (
    df_fact_joined.select(
        # Grain and transactional keys
        F.col("item_pedido_id"),
        F.col("pedido_id"),
        F.col("produto_id"),
        F.col("cliente_id"),
        F.col("canal_id"),
        F.col("fornecedor_id"),

        # Dates
        F.col("data_pedido"),
        F.col("calendar_year"),
        F.col("calendar_quarter"),
        F.col("calendar_month"),
        F.col("calendar_day"),
        F.col("calendar_month_name"),
        F.col("calendar_day_of_week"),
        F.col("calendar_day_of_week_name"),
        F.col("calendar_is_weekend"),
        F.col("calendar_is_month_start"),
        F.col("calendar_is_month_end"),

        # Customer / order context
        F.col("vendedor_id"),
        F.col("cidade_entrega"),
        F.col("estado_pedido"),
        F.col("prazo_entrega_dias"),
        F.col("tipo_cliente"),
        F.col("cliente_cidade"),
        F.col("distrito"),
        F.col("segmento"),
        F.col("potencial_valor"),
        F.col("cluster_comercial"),
        F.col("status_cliente"),

        # Product / channel context
        F.col("produto_nome"),
        F.col("categoria"),
        F.col("marca"),
        F.col("status_produto"),
        F.col("nome_canal"),
        F.col("descricao").alias("descricao_canal"),

        # Measures
        F.col("quantidade").alias("quantity_sold"),
        F.col("preco_lista_unitario"),
        F.col("desconto_unitario"),
        F.col("preco_venda_unitario"),
        F.col("custo_unitario"),
        (F.col("quantidade") * F.col("preco_lista_unitario")).alias("gross_sales_amount"),
        (F.col("quantidade") * F.col("preco_venda_unitario")).alias("net_sales_amount"),
        (F.col("quantidade") * F.col("custo_unitario")).alias("total_cost_amount"),
        ((F.col("quantidade") * F.col("preco_venda_unitario")) - (F.col("quantidade") * F.col("custo_unitario"))).alias("gross_margin_amount"),
        F.lit(1).alias("line_count"),

        # Operational / optional context
        F.col("flag_promocao"),
        F.col("lote_fornecedor"),

        # Technical lineage
        F.col("load_date").alias("item_load_date"),
        F.col("ingestion_timestamp").alias("item_ingestion_timestamp"),
        F.col("source_file").alias("item_source_file"),
        F.col("order_load_date"),
        F.col("order_ingestion_timestamp"),
        F.col("order_source_file"),
    )
)

# Derived order-level metric at item grain
order_revenue_window = F.sum("net_sales_amount").over(Window.partitionBy("pedido_id"))
df_fact_sales = df_fact_sales.withColumn("average_order_value", order_revenue_window)

print("[INFO] Gold fact dataset built successfully.")
print(f"[INFO] Row count after joins:                 {df_fact_sales.count():,}")


# ========================================
# 8. OUTPUT VALIDATION
# ========================================

print("[INFO] Validating Gold fact output...")

final_row_count = df_fact_sales.count()
expected_row_count = valid_order_items_count

duplicate_item_ids = (
    df_fact_sales
    .groupBy("item_pedido_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_produto_id_final = df_fact_sales.filter(F.col("produto_id").isNull()).count()
null_item_pedido_id_final = df_fact_sales.filter(F.col("item_pedido_id").isNull()).count()
orphan_channel_count = df_fact_sales.filter(F.col("nome_canal").isNull()).count()
orphan_calendar_count = df_fact_sales.filter(F.col("calendar_year").isNull()).count()
orphan_product_count = df_fact_sales.filter(F.col("produto_nome").isNull()).count()

print(f"[INFO] Expected row count after filtering:    {expected_row_count:,}")
print(f"[INFO] Output row count:                     {final_row_count:,}")
print(f"[INFO] Duplicate item_pedido_id count:       {duplicate_item_ids:,}")
print(f"[INFO] Null produto_id count:                {null_produto_id_final:,}")
print(f"[INFO] Null item_pedido_id count:            {null_item_pedido_id_final:,}")
print(f"[INFO] Orphan product rows:                  {orphan_product_count:,}")
print(f"[INFO] Orphan sales channel rows:            {orphan_channel_count:,}")
print(f"[INFO] Orphan calendar rows:                 {orphan_calendar_count:,}")

if final_row_count != expected_row_count:
    raise ValueError(
        f"Row count mismatch detected. Expected: {expected_row_count}, Got: {final_row_count}"
    )

if duplicate_item_ids > 0:
    raise ValueError(f"Duplicate item_pedido_id detected in output dataset: {duplicate_item_ids}")

if null_produto_id_final > 0:
    raise ValueError(f"Null produto_id detected in output dataset: {null_produto_id_final}")

if null_item_pedido_id_final > 0:
    raise ValueError(f"Null item_pedido_id detected in output dataset: {null_item_pedido_id_final}")

if orphan_channel_count > 0:
    raise ValueError(f"Orphan sales channel rows detected: {orphan_channel_count}")

if orphan_calendar_count > 0:
    raise ValueError(f"Orphan calendar rows detected: {orphan_calendar_count}")

if orphan_product_count > 0:
    raise ValueError(f"Orphan product rows detected: {orphan_product_count}")

print("[INFO] Output validation completed successfully.")


# ========================================
# 9. WRITE DELTA TABLE
# ========================================

print("[INFO] Writing Gold fact table to Delta format...")

(
    df_fact_sales.write
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
# 10. APPLY TABLE OPTIMIZATION
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
# 11. FINAL STATUS
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