# Databricks notebook source
print("hello world")

# COMMAND ----------

# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: mart_sales_performance
# ========================================

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

FACT_TABLE_NAME = "fact_sales"
DIM_CALENDAR_TABLE_NAME = "dim_calendar"
DIM_PRODUCT_TABLE_NAME = "dim_product"
DIM_SUPPLIER_TABLE_NAME = "dim_supplier"
DIM_SALES_CHANNEL_TABLE_NAME = "dim_sales_channel"

FACT_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{FACT_TABLE_NAME}"
DIM_CALENDAR_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DIM_CALENDAR_TABLE_NAME}"
DIM_PRODUCT_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DIM_PRODUCT_TABLE_NAME}"
DIM_SUPPLIER_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DIM_SUPPLIER_TABLE_NAME}"
DIM_SALES_CHANNEL_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DIM_SALES_CHANNEL_TABLE_NAME}"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_KEYS = ["data_pedido", "produto_id", "canal_id"]

AUTO_OPTIMIZE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}

REQUIRED_FACT_COLUMNS = [
    "item_pedido_id",
    "pedido_id",
    "data_pedido",
    "produto_id",
    "fornecedor_id",
    "canal_id",
    "quantity_sold",
    "gross_sales_amount",
    "net_sales_amount",
    "total_cost_amount",
    "gross_margin_amount",
    "line_count",
    "average_order_value"
]

REQUIRED_CALENDAR_COLUMNS = [
    "calendar_date",
    "calendar_year",
    "calendar_quarter",
    "calendar_month",
    "calendar_day",
    "calendar_month_name",
    "calendar_day_of_week",
    "calendar_day_name",
    "calendar_is_weekend",
    "calendar_is_month_start",
    "calendar_is_month_end"
]

REQUIRED_PRODUCT_COLUMNS = [
    "produto_id",
    "produto_nome",
    "categoria",
    "marca",
    "status_produto"
]

REQUIRED_SUPPLIER_COLUMNS = [
    "fornecedor_id",
    "nome_fornecedor",
    "pais",
    "status_fornecedor"
]

REQUIRED_SALES_CHANNEL_COLUMNS = [
    "canal_id",
    "nome_canal",
    "descricao"
]

# ========================================
# 1. CONTEXT SETUP
# ========================================

print("=" * 80)
print("STARTING GOLD PROCESSING: mart_sales_performance")
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
print(f"Dim calendar table:              {DIM_CALENDAR_TABLE}")
print(f"Dim product table:               {DIM_PRODUCT_TABLE}")
print(f"Dim supplier table:              {DIM_SUPPLIER_TABLE}")
print(f"Dim sales channel table:         {DIM_SALES_CHANNEL_TABLE}")
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
spark.sql(f"DESCRIBE TABLE {FACT_TABLE}")
spark.sql(f"DESCRIBE TABLE {DIM_CALENDAR_TABLE}")
spark.sql(f"DESCRIBE TABLE {DIM_PRODUCT_TABLE}")
spark.sql(f"DESCRIBE TABLE {DIM_SUPPLIER_TABLE}")
spark.sql(f"DESCRIBE TABLE {DIM_SALES_CHANNEL_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")

# ========================================
# 4. READ SOURCE DATA
# ========================================

df_fact_sales = spark.table(FACT_TABLE)
df_dim_calendar = spark.table(DIM_CALENDAR_TABLE)
df_dim_product = spark.table(DIM_PRODUCT_TABLE)
df_dim_supplier = spark.table(DIM_SUPPLIER_TABLE)
df_dim_sales_channel = spark.table(DIM_SALES_CHANNEL_TABLE)

print("[INFO] Source data loaded successfully.")

# ========================================
# 5. SOURCE VALIDATION
# ========================================

print("[INFO] Validating source datasets...")

missing_fact_columns = [c for c in REQUIRED_FACT_COLUMNS if c not in df_fact_sales.columns]
missing_calendar_columns = [c for c in REQUIRED_CALENDAR_COLUMNS if c not in df_dim_calendar.columns]
missing_product_columns = [c for c in REQUIRED_PRODUCT_COLUMNS if c not in df_dim_product.columns]
missing_supplier_columns = [c for c in REQUIRED_SUPPLIER_COLUMNS if c not in df_dim_supplier.columns]
missing_sales_channel_columns = [c for c in REQUIRED_SALES_CHANNEL_COLUMNS if c not in df_dim_sales_channel.columns]

print(f"[INFO] Missing fact columns:                {missing_fact_columns}")
print(f"[INFO] Missing calendar columns:            {missing_calendar_columns}")
print(f"[INFO] Missing product columns:             {missing_product_columns}")
print(f"[INFO] Missing supplier columns:            {missing_supplier_columns}")
print(f"[INFO] Missing sales channel columns:       {missing_sales_channel_columns}")

if (
    missing_fact_columns
    or missing_calendar_columns
    or missing_product_columns
    or missing_supplier_columns
    or missing_sales_channel_columns
):
    raise ValueError("Missing required columns in one or more source datasets.")

fact_quality = (
    df_fact_sales
    .select(
        F.count("*").alias("fact_row_count"),
        F.sum(F.when(F.col("data_pedido").isNull(), 1).otherwise(0)).alias("null_data_pedido"),
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_produto_id"),
        F.sum(F.when(F.col("fornecedor_id").isNull(), 1).otherwise(0)).alias("null_fornecedor_id"),
        F.sum(F.when(F.col("canal_id").isNull(), 1).otherwise(0)).alias("null_canal_id")
    )
    .collect()[0]
    .asDict()
)

print(f"[INFO] Fact row count:                      {fact_quality['fact_row_count']:,}")
print(f"[INFO] Null data_pedido count:              {fact_quality['null_data_pedido']:,}")
print(f"[INFO] Null produto_id count:               {fact_quality['null_produto_id']:,}")
print(f"[INFO] Null fornecedor_id count:            {fact_quality['null_fornecedor_id']:,}")
print(f"[INFO] Null canal_id count:                 {fact_quality['null_canal_id']:,}")

if fact_quality["null_data_pedido"] > 0:
    raise ValueError(f"Null data_pedido detected in fact_sales: {fact_quality['null_data_pedido']}")
if fact_quality["null_produto_id"] > 0:
    raise ValueError(f"Null produto_id detected in fact_sales: {fact_quality['null_produto_id']}")
if fact_quality["null_fornecedor_id"] > 0:
    raise ValueError(f"Null fornecedor_id detected in fact_sales: {fact_quality['null_fornecedor_id']}")
if fact_quality["null_canal_id"] > 0:
    raise ValueError(f"Null canal_id detected in fact_sales: {fact_quality['null_canal_id']}")

calendar_missing = (
    df_fact_sales.select("data_pedido").distinct()
    .join(
        df_dim_calendar.select(F.col("calendar_date").alias("data_pedido")).distinct(),
        on="data_pedido",
        how="left_anti"
    )
    .count()
)

product_missing = (
    df_fact_sales.select("produto_id").distinct()
    .join(df_dim_product.select("produto_id").distinct(), on="produto_id", how="left_anti")
    .count()
)

supplier_missing = (
    df_fact_sales.select("fornecedor_id").distinct()
    .join(df_dim_supplier.select("fornecedor_id").distinct(), on="fornecedor_id", how="left_anti")
    .count()
)

sales_channel_missing = (
    df_fact_sales.select("canal_id").distinct()
    .join(df_dim_sales_channel.select("canal_id").distinct(), on="canal_id", how="left_anti")
    .count()
)

print(f"[INFO] Missing calendar keys:               {calendar_missing:,}")
print(f"[INFO] Missing product keys:                {product_missing:,}")
print(f"[INFO] Missing supplier keys:               {supplier_missing:,}")
print(f"[INFO] Missing sales channel keys:          {sales_channel_missing:,}")

if calendar_missing > 0:
    raise ValueError(f"Missing calendar keys detected: {calendar_missing}")
if product_missing > 0:
    raise ValueError(f"Missing product keys detected: {product_missing}")
if supplier_missing > 0:
    raise ValueError(f"Missing supplier keys detected: {supplier_missing}")
if sales_channel_missing > 0:
    raise ValueError(f"Missing sales channel keys detected: {sales_channel_missing}")

print("[INFO] Source validation completed successfully.")

# ========================================
# 6. BUILD MART DATASET
# ========================================

print("[INFO] Building Gold mart dataset with explicit column selection...")

df_mart_sales_performance_base = (
    df_fact_sales.alias("f")
    .join(
        df_dim_calendar.alias("c"),
        F.col("f.data_pedido") == F.col("c.calendar_date"),
        how="left"
    )
    .join(
        df_dim_product.alias("p"),
        F.col("f.produto_id") == F.col("p.produto_id"),
        how="left"
    )
    .join(
        df_dim_supplier.alias("s"),
        F.col("f.fornecedor_id") == F.col("s.fornecedor_id"),
        how="left"
    )
    .join(
        df_dim_sales_channel.alias("sc"),
        F.col("f.canal_id") == F.col("sc.canal_id"),
        how="left"
    )
    .select(
        F.col("f.item_pedido_id"),
        F.col("f.pedido_id"),
        F.col("f.data_pedido"),
        F.col("f.produto_id"),
        F.col("f.fornecedor_id"),
        F.col("f.canal_id"),
        F.col("f.quantity_sold"),
        F.col("f.gross_sales_amount"),
        F.col("f.net_sales_amount"),
        F.col("f.total_cost_amount"),
        F.col("f.gross_margin_amount"),
        F.col("f.line_count"),
        F.col("f.average_order_value"),
        F.col("c.calendar_year"),
        F.col("c.calendar_quarter"),
        F.col("c.calendar_month"),
        F.col("c.calendar_day"),
        F.col("c.calendar_month_name"),
        F.col("c.calendar_day_of_week"),
        F.col("c.calendar_day_name"),
        F.col("c.calendar_is_weekend"),
        F.col("c.calendar_is_month_start"),
        F.col("c.calendar_is_month_end"),
        F.col("p.produto_nome"),
        F.col("p.categoria"),
        F.col("p.marca"),
        F.col("p.status_produto"),
        F.col("s.nome_fornecedor"),
        F.col("s.pais").alias("fornecedor_pais"),
        F.col("s.status_fornecedor"),
        F.col("sc.nome_canal"),
        F.col("sc.descricao").alias("descricao_canal")
    )
)

df_mart_sales_performance = (
    df_mart_sales_performance_base
    .groupBy(
        "data_pedido",
        "produto_id",
        "canal_id",
        "calendar_year",
        "calendar_quarter",
        "calendar_month",
        "calendar_day",
        "calendar_month_name",
        "calendar_day_of_week",
        "calendar_day_name",
        "calendar_is_weekend",
        "calendar_is_month_start",
        "calendar_is_month_end",
        "produto_nome",
        "categoria",
        "marca",
        "status_produto",
        "nome_fornecedor",
        "fornecedor_pais",
        "status_fornecedor",
        "nome_canal",
        "descricao_canal"
    )
    .agg(
        F.round(F.sum("quantity_sold"), 2).alias("quantity_sold"),
        F.round(F.sum("gross_sales_amount"), 2).alias("gross_sales_amount"),
        F.round(F.sum("net_sales_amount"), 2).alias("net_sales_amount"),
        F.round(F.sum("total_cost_amount"), 2).alias("total_cost_amount"),
        F.round(F.sum("gross_margin_amount"), 2).alias("gross_margin_amount"),
        F.countDistinct("pedido_id").alias("order_count"),
        F.sum("line_count").alias("line_count"),
        F.round(F.avg("average_order_value"), 2).alias("average_order_value")
    )
)

print("[INFO] Gold mart dataset built successfully.")

# ========================================
# 7. OUTPUT VALIDATION
# ========================================

print("[INFO] Validating Gold mart output...")

df_mart_sales_performance = df_mart_sales_performance.cache()
_ = df_mart_sales_performance.count()

expected_row_count = (
    df_mart_sales_performance_base
    .select("data_pedido", "produto_id", "canal_id")
    .distinct()
    .count()
)

final_quality = (
    df_mart_sales_performance
    .select(
        F.count("*").alias("final_row_count"),
        F.countDistinct(
            F.concat_ws("||", F.col("data_pedido"), F.col("produto_id"), F.col("canal_id"))
        ).alias("distinct_grain_keys"),
        F.sum(F.when(F.col("data_pedido").isNull(), 1).otherwise(0)).alias("null_data_pedido"),
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_produto_id"),
        F.sum(F.when(F.col("canal_id").isNull(), 1).otherwise(0)).alias("null_canal_id"),
        F.sum(F.when(F.col("quantity_sold").isNull(), 1).otherwise(0)).alias("null_quantity_sold"),
        F.sum(F.when(F.col("gross_sales_amount").isNull(), 1).otherwise(0)).alias("null_gross_sales_amount"),
        F.sum(F.when(F.col("net_sales_amount").isNull(), 1).otherwise(0)).alias("null_net_sales_amount"),
        F.sum(F.when(F.col("total_cost_amount").isNull(), 1).otherwise(0)).alias("null_total_cost_amount"),
        F.sum(F.when(F.col("gross_margin_amount").isNull(), 1).otherwise(0)).alias("null_gross_margin_amount"),
        F.sum(F.when(F.col("order_count").isNull(), 1).otherwise(0)).alias("null_order_count"),
        F.sum(F.when(F.col("line_count").isNull(), 1).otherwise(0)).alias("null_line_count"),
        F.sum(F.when(F.col("average_order_value").isNull(), 1).otherwise(0)).alias("null_average_order_value")
    )
    .collect()[0]
    .asDict()
)

duplicate_grain_count = final_quality["final_row_count"] - final_quality["distinct_grain_keys"]

fact_totals = (
    df_fact_sales
    .select(
        F.round(F.sum("quantity_sold"), 2).alias("fact_quantity_sold"),
        F.round(F.sum("gross_sales_amount"), 2).alias("fact_gross_sales_amount"),
        F.round(F.sum("net_sales_amount"), 2).alias("fact_net_sales_amount"),
        F.round(F.sum("total_cost_amount"), 2).alias("fact_total_cost_amount"),
        F.round(F.sum("gross_margin_amount"), 2).alias("fact_gross_margin_amount"),
        F.countDistinct("pedido_id").alias("fact_order_count"),
        F.sum("line_count").alias("fact_line_count")
    )
    .collect()[0]
    .asDict()
)

mart_totals = (
    df_mart_sales_performance
    .select(
        F.round(F.sum("quantity_sold"), 2).alias("mart_quantity_sold"),
        F.round(F.sum("gross_sales_amount"), 2).alias("mart_gross_sales_amount"),
        F.round(F.sum("net_sales_amount"), 2).alias("mart_net_sales_amount"),
        F.round(F.sum("total_cost_amount"), 2).alias("mart_total_cost_amount"),
        F.round(F.sum("gross_margin_amount"), 2).alias("mart_gross_margin_amount"),
        F.sum("order_count").alias("mart_order_count"),
        F.sum("line_count").alias("mart_line_count")
    )
    .collect()[0]
    .asDict()
)

print(f"[INFO] Expected row count:                   {expected_row_count:,}")
print(f"[INFO] Output row count:                     {final_quality['final_row_count']:,}")
print(f"[INFO] Distinct grain keys:                  {final_quality['distinct_grain_keys']:,}")
print(f"[INFO] Duplicate grain count:               {duplicate_grain_count:,}")
print(f"[INFO] Null data_pedido count:              {final_quality['null_data_pedido']:,}")
print(f"[INFO] Null produto_id count:               {final_quality['null_produto_id']:,}")
print(f"[INFO] Null canal_id count:                 {final_quality['null_canal_id']:,}")
print(f"[INFO] Null quantity_sold count:            {final_quality['null_quantity_sold']:,}")
print(f"[INFO] Null gross_sales_amount count:       {final_quality['null_gross_sales_amount']:,}")
print(f"[INFO] Null net_sales_amount count:         {final_quality['null_net_sales_amount']:,}")
print(f"[INFO] Null total_cost_amount count:        {final_quality['null_total_cost_amount']:,}")
print(f"[INFO] Null gross_margin_amount count:      {final_quality['null_gross_margin_amount']:,}")
print(f"[INFO] Null order_count count:              {final_quality['null_order_count']:,}")
print(f"[INFO] Null line_count count:               {final_quality['null_line_count']:,}")
print(f"[INFO] Null average_order_value count:      {final_quality['null_average_order_value']:,}")
print(f"[INFO] Fact quantity_sold total:            {fact_totals['fact_quantity_sold']}")
print(f"[INFO] Mart quantity_sold total:            {mart_totals['mart_quantity_sold']}")
print(f"[INFO] Fact gross_sales_amount total:       {fact_totals['fact_gross_sales_amount']}")
print(f"[INFO] Mart gross_sales_amount total:       {mart_totals['mart_gross_sales_amount']}")
print(f"[INFO] Fact net_sales_amount total:         {fact_totals['fact_net_sales_amount']}")
print(f"[INFO] Mart net_sales_amount total:         {mart_totals['mart_net_sales_amount']}")
print(f"[INFO] Fact total_cost_amount total:        {fact_totals['fact_total_cost_amount']}")
print(f"[INFO] Mart total_cost_amount total:        {mart_totals['mart_total_cost_amount']}")
print(f"[INFO] Fact gross_margin_amount total:      {fact_totals['fact_gross_margin_amount']}")
print(f"[INFO] Mart gross_margin_amount total:      {mart_totals['mart_gross_margin_amount']}")
print(f"[INFO] Fact line_count total:               {fact_totals['fact_line_count']}")
print(f"[INFO] Mart line_count total:               {mart_totals['mart_line_count']}")
print(f"[INFO] Fact distinct order count:           {fact_totals['fact_order_count']}")
print(f"[INFO] Mart aggregated order_count total:   {mart_totals['mart_order_count']}")

if final_quality["final_row_count"] != expected_row_count:
    raise ValueError(
        f"Row count mismatch detected. Expected: {expected_row_count}, Got: {final_quality['final_row_count']}"
    )

if duplicate_grain_count > 0:
    raise ValueError(f"Duplicate grain detected in output dataset: {duplicate_grain_count}")

if final_quality["null_data_pedido"] > 0:
    raise ValueError(f"Null data_pedido detected in output dataset: {final_quality['null_data_pedido']}")
if final_quality["null_produto_id"] > 0:
    raise ValueError(f"Null produto_id detected in output dataset: {final_quality['null_produto_id']}")
if final_quality["null_canal_id"] > 0:
    raise ValueError(f"Null canal_id detected in output dataset: {final_quality['null_canal_id']}")
if final_quality["null_quantity_sold"] > 0:
    raise ValueError(f"Null quantity_sold detected in output dataset: {final_quality['null_quantity_sold']}")
if final_quality["null_gross_sales_amount"] > 0:
    raise ValueError(f"Null gross_sales_amount detected in output dataset: {final_quality['null_gross_sales_amount']}")
if final_quality["null_net_sales_amount"] > 0:
    raise ValueError(f"Null net_sales_amount detected in output dataset: {final_quality['null_net_sales_amount']}")
if final_quality["null_total_cost_amount"] > 0:
    raise ValueError(f"Null total_cost_amount detected in output dataset: {final_quality['null_total_cost_amount']}")
if final_quality["null_gross_margin_amount"] > 0:
    raise ValueError(f"Null gross_margin_amount detected in output dataset: {final_quality['null_gross_margin_amount']}")
if final_quality["null_order_count"] > 0:
    raise ValueError(f"Null order_count detected in output dataset: {final_quality['null_order_count']}")
if final_quality["null_line_count"] > 0:
    raise ValueError(f"Null line_count detected in output dataset: {final_quality['null_line_count']}")
if final_quality["null_average_order_value"] > 0:
    raise ValueError(f"Null average_order_value detected in output dataset: {final_quality['null_average_order_value']}")

if mart_totals["mart_quantity_sold"] != fact_totals["fact_quantity_sold"]:
    raise ValueError("Metric reconciliation failed for quantity_sold.")
if mart_totals["mart_gross_sales_amount"] != fact_totals["fact_gross_sales_amount"]:
    raise ValueError("Metric reconciliation failed for gross_sales_amount.")
if mart_totals["mart_net_sales_amount"] != fact_totals["fact_net_sales_amount"]:
    raise ValueError("Metric reconciliation failed for net_sales_amount.")
if mart_totals["mart_total_cost_amount"] != fact_totals["fact_total_cost_amount"]:
    raise ValueError("Metric reconciliation failed for total_cost_amount.")
if mart_totals["mart_gross_margin_amount"] != fact_totals["fact_gross_margin_amount"]:
    raise ValueError("Metric reconciliation failed for gross_margin_amount.")
if mart_totals["mart_line_count"] != fact_totals["fact_line_count"]:
    raise ValueError("Metric reconciliation failed for line_count.")

print("[INFO] Output validation completed successfully.")
print("[INFO] Order count difference is expected due to non-additive behavior at the mart grain.")

# ========================================
# 8. WRITE DELTA TABLE
# ========================================

print("[INFO] Writing Gold mart table to Delta format...")

(
    df_mart_sales_performance.write
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