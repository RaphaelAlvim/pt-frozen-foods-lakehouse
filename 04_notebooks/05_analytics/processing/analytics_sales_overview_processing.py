# Databricks notebook source
# ========================================
# ANALYTICS PROCESSING NOTEBOOK
# DATASET: analytics_sales_overview
# ========================================

# Processing decisions:
# - Use CTAS (CREATE OR REPLACE TABLE AS SELECT) to create and register the Delta table in one step.
# - Use Liquid Clustering to optimize recurring queries by customer, product, and channel.
# - Avoid unnecessary cache, repartition, or coalesce operations.
# - Keep only essential final validations to control processing cost.
# - Run OPTIMIZE after table creation to improve file layout for analytical consumption.

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "gold"
TARGET_SCHEMA = "gold"

DOMAIN = "analytics"
DATASET = "analytics_sales_overview"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.fact_sales"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

GRAIN_COLUMNS = [
    "data_pedido",
    "cliente_id",
    "produto_id",
    "canal_id"
]

CLUSTER_COLUMNS = [
    "cliente_id",
    "produto_id",
    "canal_id"
]

print("=" * 80)
print("STARTING ANALYTICS PROCESSING: analytics_sales_overview")
print("=" * 80)

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

# ========================================
# 1. CREATE ANALYTICS TABLE
# ========================================

print("[INFO] Creating analytics Delta table using CTAS...")

spark.sql(f"""
CREATE OR REPLACE TABLE {TARGET_TABLE}
USING DELTA
LOCATION '{TARGET_PATH}'
CLUSTER BY ({", ".join(CLUSTER_COLUMNS)})
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
SELECT
    data_pedido,
    calendar_year,
    calendar_quarter,
    calendar_month,
    calendar_month_name,

    cliente_id,
    tipo_cliente,
    cliente_cidade,
    distrito,
    segmento,
    potencial_valor,
    cluster_comercial,
    status_cliente,

    produto_id,
    produto_nome,
    categoria,
    marca,
    status_produto,

    canal_id,
    nome_canal,
    descricao_canal,

    SUM(quantity_sold) AS quantity_sold,
    SUM(gross_sales_amount) AS gross_sales_amount,
    SUM(net_sales_amount) AS net_sales_amount,
    SUM(total_cost_amount) AS total_cost_amount,
    SUM(gross_margin_amount) AS gross_margin_amount,
    COUNT(DISTINCT pedido_id) AS order_count,
    SUM(line_count) AS line_count

FROM {SOURCE_TABLE}

GROUP BY
    data_pedido,
    calendar_year,
    calendar_quarter,
    calendar_month,
    calendar_month_name,

    cliente_id,
    tipo_cliente,
    cliente_cidade,
    distrito,
    segmento,
    potencial_valor,
    cluster_comercial,
    status_cliente,

    produto_id,
    produto_nome,
    categoria,
    marca,
    status_produto,

    canal_id,
    nome_canal,
    descricao_canal
""")

print("[INFO] Analytics table created successfully.")

# ========================================
# 2. OPTIMIZATION
# ========================================

print("[INFO] Running OPTIMIZE for clustered Delta layout...")

spark.sql(f"OPTIMIZE {TARGET_TABLE}")

print("[INFO] Table optimization completed.")

# ========================================
# 3. FINAL VALIDATIONS
# ========================================

print("=" * 80)
print("FINAL VALIDATIONS")
print("=" * 80)

target_df = spark.table(TARGET_TABLE)

total_rows = target_df.count()

distinct_grain_rows = (
    target_df
    .select(GRAIN_COLUMNS)
    .distinct()
    .count()
)

duplicate_rows = total_rows - distinct_grain_rows

null_check = target_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in GRAIN_COLUMNS + ["net_sales_amount"]
]).collect()[0].asDict()

print(f"Source table:          {SOURCE_TABLE}")
print(f"Target table:          {TARGET_TABLE}")
print(f"Target path:           {TARGET_PATH}")
print(f"Total rows:            {total_rows:,}")
print(f"Distinct grain rows:   {distinct_grain_rows:,}")
print(f"Duplicate grain rows:  {duplicate_rows:,}")
print(f"Null check:            {null_check}")

if duplicate_rows != 0:
    raise ValueError("Duplicate grain records found in analytics dataset.")

if any(value != 0 for value in null_check.values()):
    raise ValueError("Null values found in critical analytics columns.")

print("[INFO] Final validations passed.")

print("=" * 80)
print("ANALYTICS PROCESSING COMPLETED SUCCESSFULLY")
print("=" * 80)