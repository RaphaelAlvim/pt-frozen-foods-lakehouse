# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: mart_sales_performance
# ========================================

# Processing decisions:
# - Use CTAS (CREATE OR REPLACE TABLE AS SELECT) to create and register the Delta table in one step.
# - Use Liquid Clustering to optimize recurring queries by date, product, and channel.
# - Avoid unnecessary cache, repartition, or coalesce operations.
# - Keep only essential validations to control processing cost.
# - Use tolerance-based metric reconciliation for floating-point measures.

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "gold"
TARGET_SCHEMA = "gold"

DOMAIN = "marts"
DATASET = "mart_sales_performance"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.fact_sales"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

GRAIN_COLUMNS = [
    "data_pedido",
    "produto_id",
    "canal_id"
]

CLUSTER_COLUMNS = [
    "data_pedido",
    "produto_id",
    "canal_id"
]

REQUIRED_COLUMNS = [
    "pedido_id",
    "data_pedido",
    "produto_id",
    "canal_id",
    "fornecedor_id",
    "calendar_year",
    "calendar_quarter",
    "calendar_month",
    "calendar_day",
    "calendar_month_name",
    "calendar_day_of_week",
    "calendar_day_of_week_name",
    "calendar_is_weekend",
    "calendar_is_month_start",
    "calendar_is_month_end",
    "produto_nome",
    "categoria",
    "marca",
    "status_produto",
    "nome_canal",
    "descricao_canal",
    "quantity_sold",
    "gross_sales_amount",
    "net_sales_amount",
    "total_cost_amount",
    "gross_margin_amount",
    "line_count",
    "order_total_amount"
]

TOLERANCE = 0.01

print("=" * 80)
print("STARTING GOLD PROCESSING: mart_sales_performance")
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

df_source = spark.table(SOURCE_TABLE)

missing_columns = [c for c in REQUIRED_COLUMNS if c not in df_source.columns]

if missing_columns:
    raise ValueError(f"Missing required columns in source dataset: {missing_columns}")

print("[INFO] Required columns validation completed successfully.")

# ========================================
# 4. CREATE MART TABLE
# ========================================

print("[INFO] Creating Gold mart table using CTAS...")

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
    data_pedido,
    produto_id,
    canal_id,

    calendar_year,
    calendar_quarter,
    calendar_month,
    calendar_day,
    calendar_month_name,
    calendar_day_of_week,
    calendar_day_of_week_name,
    calendar_is_weekend,
    calendar_is_month_start,
    calendar_is_month_end,

    produto_nome,
    categoria,
    marca,
    status_produto,
    fornecedor_id,

    nome_canal,
    descricao_canal,

    SUM(quantity_sold) AS quantity_sold,
    SUM(gross_sales_amount) AS gross_sales_amount,
    SUM(net_sales_amount) AS net_sales_amount,
    SUM(total_cost_amount) AS total_cost_amount,
    SUM(gross_margin_amount) AS gross_margin_amount,
    COUNT(DISTINCT pedido_id) AS order_count,
    SUM(line_count) AS line_count,
    AVG(order_total_amount) AS avg_order_total_amount

FROM {SOURCE_TABLE}

GROUP BY
    data_pedido,
    produto_id,
    canal_id,

    calendar_year,
    calendar_quarter,
    calendar_month,
    calendar_day,
    calendar_month_name,
    calendar_day_of_week,
    calendar_day_of_week_name,
    calendar_is_weekend,
    calendar_is_month_start,
    calendar_is_month_end,

    produto_nome,
    categoria,
    marca,
    status_produto,
    fornecedor_id,

    nome_canal,
    descricao_canal
""")

print("[INFO] Gold mart table created successfully.")

# ========================================
# 5. OPTIMIZATION
# ========================================

# OPTIMIZE is executed after the full rebuild because this mart is intended
# for recurring analytical consumption.
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
        F.countDistinct(*[F.col(c) for c in GRAIN_COLUMNS]).alias("distinct_grain_rows"),
        F.sum(F.when(F.col("data_pedido").isNull(), 1).otherwise(0)).alias("null_data_pedido"),
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_produto_id"),
        F.sum(F.when(F.col("canal_id").isNull(), 1).otherwise(0)).alias("null_canal_id")
    )
    .collect()[0]
)

duplicate_rows = validation["row_count"] - validation["distinct_grain_rows"]

print(f"Total rows:            {validation['row_count']:,}")
print(f"Distinct grain rows:   {validation['distinct_grain_rows']:,}")
print(f"Duplicate grain rows:  {duplicate_rows:,}")
print(f"Null data_pedido:      {validation['null_data_pedido']}")
print(f"Null produto_id:       {validation['null_produto_id']}")
print(f"Null canal_id:         {validation['null_canal_id']}")

if duplicate_rows != 0:
    raise ValueError("Grain violation detected: duplicate grain records found.")

if validation["null_data_pedido"] > 0:
    raise ValueError("Null values detected in data_pedido.")

if validation["null_produto_id"] > 0:
    raise ValueError("Null values detected in produto_id.")

if validation["null_canal_id"] > 0:
    raise ValueError("Null values detected in canal_id.")

print("[INFO] Grain and null validations completed successfully.")

# ========================================
# 7. METRIC RECONCILIATION
# ========================================

print("[INFO] Running metric reconciliation...")

fact_totals = (
    df_source
    .agg(
        F.sum("quantity_sold").alias("fact_quantity_sold"),
        F.sum("net_sales_amount").alias("fact_net_sales_amount"),
        F.sum("gross_margin_amount").alias("fact_gross_margin_amount"),
        F.sum("line_count").alias("fact_line_count")
    )
    .collect()[0]
)

mart_totals = (
    df_target
    .agg(
        F.sum("quantity_sold").alias("mart_quantity_sold"),
        F.sum("net_sales_amount").alias("mart_net_sales_amount"),
        F.sum("gross_margin_amount").alias("mart_gross_margin_amount"),
        F.sum("line_count").alias("mart_line_count")
    )
    .collect()[0]
)

quantity_diff = abs(fact_totals["fact_quantity_sold"] - mart_totals["mart_quantity_sold"])
net_sales_diff = abs(fact_totals["fact_net_sales_amount"] - mart_totals["mart_net_sales_amount"])
gross_margin_diff = abs(fact_totals["fact_gross_margin_amount"] - mart_totals["mart_gross_margin_amount"])
line_count_diff = abs(fact_totals["fact_line_count"] - mart_totals["mart_line_count"])

print(f"Quantity sold difference:       {quantity_diff:,.4f}")
print(f"Net sales amount difference:    {net_sales_diff:,.4f}")
print(f"Gross margin amount difference: {gross_margin_diff:,.4f}")
print(f"Line count difference:          {line_count_diff:,.4f}")

if quantity_diff > TOLERANCE:
    raise ValueError("Mismatch in quantity_sold.")

if net_sales_diff > TOLERANCE:
    raise ValueError("Mismatch in net_sales_amount.")

if gross_margin_diff > TOLERANCE:
    raise ValueError("Mismatch in gross_margin_amount.")

if line_count_diff > TOLERANCE:
    raise ValueError("Mismatch in line_count.")

print("[INFO] Metric reconciliation completed successfully.")
print("[INFO] Order count is intentionally not reconciled globally because it is non-additive at this mart grain.")

# ========================================
# 8. FINAL TABLE DETAIL
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