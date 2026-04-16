# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: mart_customer_product_mix
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "gold"
TARGET_SCHEMA = "gold"

DOMAIN = "marts"
DATASET = "mart_customer_product_mix"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

FACT_TABLE_NAME = "fact_sales"

FACT_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{FACT_TABLE_NAME}"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_KEYS = ["data_pedido", "cliente_id", "produto_id"]

AUTO_OPTIMIZE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}

REQUIRED_COLUMNS = [
    "pedido_id",
    "data_pedido",
    "cliente_id",
    "produto_id",
    "tipo_cliente",
    "cliente_cidade",
    "distrito",
    "segmento",
    "potencial_valor",
    "cluster_comercial",
    "status_cliente",
    "produto_nome",
    "categoria",
    "marca",
    "status_produto",
    "quantity_sold",
    "gross_sales_amount",
    "net_sales_amount",
    "total_cost_amount",
    "gross_margin_amount",
    "line_count",
    "average_order_value"
]


# ========================================
# 1. CONTEXT SETUP
# ========================================

print("=" * 80)
print("STARTING GOLD PROCESSING: mart_customer_product_mix")
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
print(f"Fact table:                      {FACT_TABLE}")
print(f"Target table:                    {TARGET_TABLE}")
print(f"Target path:                     {TARGET_PATH}")
print(f"Cluster keys:                    {', '.join(CLUSTER_KEYS)}")
print(f"Optimization strategy:           Delta Auto Optimize + Liquid Clustering")
print(f"Partitioning strategy:           None")
print(f"Optimization rationale:          Align storage layout with mart grain and BI access pattern")
print("=" * 80)


# ========================================
# 3. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {FACT_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")


# ========================================
# 4. READ SOURCE DATA
# ========================================

df_fact_sales = spark.table(FACT_TABLE)

print("[INFO] Source data loaded successfully.")


# ========================================
# 5. SOURCE VALIDATION (ESSENTIAL ONLY)
# ========================================

missing_columns = [c for c in REQUIRED_COLUMNS if c not in df_fact_sales.columns]

if missing_columns:
    raise ValueError(f"Missing required columns in source dataset: {missing_columns}")

print("[INFO] Required columns validation completed successfully.")


# ========================================
# 6. BUILD MART (CORE LOGIC)
# ========================================

df_mart = (
    df_fact_sales
    .groupBy(
        "data_pedido",
        "cliente_id",
        "produto_id",
        "tipo_cliente",
        "cliente_cidade",
        "distrito",
        "segmento",
        "potencial_valor",
        "cluster_comercial",
        "status_cliente",
        "produto_nome",
        "categoria",
        "marca",
        "status_produto"
    )
    .agg(
        F.sum("quantity_sold").alias("quantity_sold"),
        F.sum("gross_sales_amount").alias("gross_sales_amount"),
        F.sum("net_sales_amount").alias("net_sales_amount"),
        F.sum("total_cost_amount").alias("total_cost_amount"),
        F.sum("gross_margin_amount").alias("gross_margin_amount"),
        F.countDistinct("pedido_id").alias("order_count"),
        F.sum("line_count").alias("line_count"),
        F.avg("average_order_value").alias("average_order_value")
    )
)

print("[INFO] Mart dataset built successfully.")


# ========================================
# 7. OUTPUT VALIDATION (CRITICAL AND LIGHTWEIGHT)
# ========================================

validation = (
    df_mart
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct(
            F.concat_ws("||", "data_pedido", "cliente_id", "produto_id")
        ).alias("distinct_keys"),
        F.sum(F.when(F.col("data_pedido").isNull(), 1).otherwise(0)).alias("null_data_pedido"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id"),
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_produto_id")
    )
    .collect()[0]
)

if validation["row_count"] != validation["distinct_keys"]:
    raise ValueError("Grain violation detected: duplicate keys found.")

if validation["null_data_pedido"] > 0:
    raise ValueError("Null values detected in data_pedido.")

if validation["null_cliente_id"] > 0:
    raise ValueError("Null values detected in cliente_id.")

if validation["null_produto_id"] > 0:
    raise ValueError("Null values detected in produto_id.")

print("[INFO] Output validation completed successfully.")


# ========================================
# 8. METRIC RECONCILIATION (ESSENTIAL ONLY)
# ========================================

fact_totals = (
    df_fact_sales
    .agg(
        F.sum("quantity_sold").alias("fact_quantity_sold"),
        F.sum("net_sales_amount").alias("fact_net_sales_amount"),
        F.sum("gross_margin_amount").alias("fact_gross_margin_amount"),
        F.sum("line_count").alias("fact_line_count")
    )
    .collect()[0]
)

mart_totals = (
    df_mart
    .agg(
        F.sum("quantity_sold").alias("mart_quantity_sold"),
        F.sum("net_sales_amount").alias("mart_net_sales_amount"),
        F.sum("gross_margin_amount").alias("mart_gross_margin_amount"),
        F.sum("line_count").alias("mart_line_count")
    )
    .collect()[0]
)

if fact_totals["fact_quantity_sold"] != mart_totals["mart_quantity_sold"]:
    raise ValueError("Mismatch in quantity_sold.")

if fact_totals["fact_net_sales_amount"] != mart_totals["mart_net_sales_amount"]:
    raise ValueError("Mismatch in net_sales_amount.")

if fact_totals["fact_gross_margin_amount"] != mart_totals["mart_gross_margin_amount"]:
    raise ValueError("Mismatch in gross_margin_amount.")

if fact_totals["fact_line_count"] != mart_totals["mart_line_count"]:
    raise ValueError("Mismatch in line_count.")

print("[INFO] Metric reconciliation completed successfully.")
print("[INFO] Order count is intentionally not reconciled globally because it is non-additive at this mart grain.")


# ========================================
# 9. WRITE DELTA TABLE
# ========================================

(
    df_mart.write
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
# 10. OPTIMIZATION
# ========================================

print("=" * 80)
print("APPLYING TABLE OPTIMIZATION STRATEGIES")
print("=" * 80)

print("[INFO] Optimization strategy 1: Delta Auto Optimize")
print("[INFO] - optimizeWrite: reduces small file generation during writes")
print("[INFO] - autoCompact: compacts small files automatically after writes")

for property_name, property_value in AUTO_OPTIMIZE_PROPERTIES.items():
    spark.sql(f"""
        ALTER TABLE {TARGET_TABLE}
        SET TBLPROPERTIES ('{property_name}' = '{property_value}')
    """)

print("[INFO] Delta Auto Optimize properties applied successfully.")

print("[INFO] Optimization strategy 2: Liquid Clustering")
print(f"[INFO] - clustering columns: {', '.join(CLUSTER_KEYS)}")
print("[INFO] - purpose: improve query pruning and read performance for the mart grain")
print("[INFO] - rationale: these columns match the analytical access pattern and validated grain")

spark.sql(f"ALTER TABLE {TARGET_TABLE} CLUSTER BY ({', '.join(CLUSTER_KEYS)})")

print("[INFO] Liquid Clustering applied successfully.")

print("[INFO] Optimization strategy summary:")
print("[INFO] - no partitioning was used")
print("[INFO] - Liquid Clustering was preferred for flexibility and maintenance simplicity")
print("[INFO] - optimization aligned with mart grain and expected BI query patterns")
print("=" * 80)


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