# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: erp_order_items
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "erp"
DATASET = "erp_order_items"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_COLUMNS = [
    "pedido_id",
    "produto_id",
    "item_pedido_id"
]

REQUIRED_COLUMNS = [
    "item_pedido_id",
    "pedido_id",
    "produto_id",
    "quantidade",
    "preco_lista_unitario",
    "desconto_unitario",
    "preco_venda_unitario",
    "custo_unitario",
    "lote_fornecedor",
    "flag_promocao",
    "nota_item",
    "load_date",
    "ingestion_timestamp",
    "source_file"
]

print("=" * 80)
print("STARTING SILVER PROCESSING: erp_order_items")
print("=" * 80)

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print("[INFO] Context setup completed successfully.")

# ========================================
# 1. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {SOURCE_TABLE}")

print("[INFO] Checking source path access...")
source_items = dbutils.fs.ls(SOURCE_PATH)

if len(source_items) == 0:
    raise ValueError(f"No files found in source path: {SOURCE_PATH}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")

# ========================================
# 2. SOURCE VALIDATION
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
        F.countDistinct("item_pedido_id").alias("distinct_item_pedido_ids"),
        F.sum(F.when(F.col("item_pedido_id").isNull(), 1).otherwise(0)).alias("null_item_pedido_id"),
        F.sum(F.when(F.col("pedido_id").isNull(), 1).otherwise(0)).alias("null_pedido_id"),
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_produto_id"),
        F.sum(F.when(F.col("quantidade").isNull(), 1).otherwise(0)).alias("null_quantidade"),
        F.sum(F.when(F.col("preco_venda_unitario").isNull(), 1).otherwise(0)).alias("null_preco_venda_unitario"),
        F.sum(F.when(F.col("custo_unitario").isNull(), 1).otherwise(0)).alias("null_custo_unitario")
    )
    .collect()[0]
)

source_duplicate_item_pedido_ids = source_validation["row_count"] - source_validation["distinct_item_pedido_ids"]

print(f"Source row count:                  {source_validation['row_count']:,}")
print(f"Duplicate item_pedido_id records:  {source_duplicate_item_pedido_ids:,}")
print(f"Null item_pedido_id:               {source_validation['null_item_pedido_id']:,}")
print(f"Null pedido_id:                    {source_validation['null_pedido_id']:,}")
print(f"Null produto_id:                   {source_validation['null_produto_id']:,}")
print(f"Null quantidade:                   {source_validation['null_quantidade']:,}")
print(f"Null preco_venda_unitario:         {source_validation['null_preco_venda_unitario']:,}")
print(f"Null custo_unitario:               {source_validation['null_custo_unitario']:,}")

if source_validation["row_count"] == 0:
    raise ValueError("Source dataset is empty.")

source_null_failures = {
    "item_pedido_id": source_validation["null_item_pedido_id"],
    "pedido_id": source_validation["null_pedido_id"],
    "produto_id": source_validation["null_produto_id"],
    "quantidade": source_validation["null_quantidade"],
    "preco_venda_unitario": source_validation["null_preco_venda_unitario"],
    "custo_unitario": source_validation["null_custo_unitario"]
}

source_null_failures = {column: count for column, count in source_null_failures.items() if count > 0}

if source_null_failures:
    raise ValueError(f"Null values detected in source critical columns: {source_null_failures}")

print("[INFO] Source validation completed successfully.")

# ========================================
# 3. CREATE SILVER TABLE
# ========================================

print("[INFO] Creating Silver table using CTAS...")

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
WITH standardized AS (
    SELECT
        NULLIF(TRIM(item_pedido_id), '') AS item_pedido_id,
        NULLIF(TRIM(pedido_id), '') AS pedido_id,
        NULLIF(TRIM(produto_id), '') AS produto_id,

        CAST(quantidade AS INT) AS quantidade,
        CAST(preco_lista_unitario AS DECIMAL(18, 4)) AS preco_lista_unitario,
        CAST(desconto_unitario AS DECIMAL(18, 4)) AS desconto_unitario,
        CAST(preco_venda_unitario AS DECIMAL(18, 4)) AS preco_venda_unitario,
        CAST(custo_unitario AS DECIMAL(18, 4)) AS custo_unitario,

        NULLIF(TRIM(lote_fornecedor), '') AS lote_fornecedor,
        CAST(flag_promocao AS BOOLEAN) AS flag_promocao,
        NULLIF(TRIM(nota_item), '') AS nota_item,

        load_date,
        ingestion_timestamp,
        source_file

    FROM {SOURCE_TABLE}
),

deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY item_pedido_id
                ORDER BY ingestion_timestamp DESC, load_date DESC, source_file DESC
            ) AS rn
        FROM standardized
    )
    WHERE rn = 1
)

SELECT
    item_pedido_id,
    pedido_id,
    produto_id,
    quantidade,
    preco_lista_unitario,
    desconto_unitario,
    preco_venda_unitario,
    custo_unitario,
    lote_fornecedor,
    flag_promocao,
    nota_item,
    load_date,
    ingestion_timestamp,
    source_file

FROM deduplicated
""")

print("[INFO] Silver table created successfully.")

# ========================================
# 4. OPTIMIZATION
# ========================================

print("[INFO] Running OPTIMIZE...")

spark.sql(f"OPTIMIZE {TARGET_TABLE}")

print("[INFO] Optimization completed.")

# ========================================
# 5. FINAL VALIDATIONS
# ========================================

print("=" * 80)
print("FINAL VALIDATIONS")
print("=" * 80)

df_target = spark.table(TARGET_TABLE)

final = (
    df_target
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("item_pedido_id").alias("distinct_item_pedido_ids"),
        F.sum(F.when(F.col("item_pedido_id").isNull(), 1).otherwise(0)).alias("null_item_pedido_id"),
        F.sum(F.when(F.col("pedido_id").isNull(), 1).otherwise(0)).alias("null_pedido_id"),
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_produto_id"),
        F.sum(F.when(F.col("quantidade").isNull(), 1).otherwise(0)).alias("null_quantidade"),
        F.sum(F.when(F.col("preco_venda_unitario").isNull(), 1).otherwise(0)).alias("null_preco_venda_unitario"),
        F.sum(F.when(F.col("custo_unitario").isNull(), 1).otherwise(0)).alias("null_custo_unitario")
    )
    .collect()[0]
)

duplicates = final["row_count"] - final["distinct_item_pedido_ids"]

print(f"Rows:                          {final['row_count']:,}")
print(f"Duplicate item_pedido_id:      {duplicates:,}")
print(f"Null item_pedido_id:           {final['null_item_pedido_id']}")
print(f"Null pedido_id:                {final['null_pedido_id']}")
print(f"Null produto_id:               {final['null_produto_id']}")
print(f"Null quantidade:               {final['null_quantidade']}")
print(f"Null preco_venda_unitario:     {final['null_preco_venda_unitario']}")
print(f"Null custo_unitario:           {final['null_custo_unitario']}")

if final["row_count"] == 0:
    raise ValueError("Silver dataset is empty.")

if duplicates > 0:
    raise ValueError("Duplicate item_pedido_id detected after deduplication.")

critical_nulls = {
    "item_pedido_id": final["null_item_pedido_id"],
    "pedido_id": final["null_pedido_id"],
    "produto_id": final["null_produto_id"],
    "quantidade": final["null_quantidade"],
    "preco_venda_unitario": final["null_preco_venda_unitario"],
    "custo_unitario": final["null_custo_unitario"]
}

null_failures = {column: count for column, count in critical_nulls.items() if count > 0}

if null_failures:
    raise ValueError(f"Null values detected in critical columns: {null_failures}")

print("[INFO] Final validations completed.")

# ========================================
# 6. FINAL STATUS
# ========================================

detail = spark.sql(f"DESCRIBE DETAIL {TARGET_TABLE}").collect()[0].asDict()

print("=" * 80)
print("FINAL TABLE DETAIL")
print("=" * 80)
print(f"Files: {detail.get('numFiles')}")
print(f"Size:  {detail.get('sizeInBytes')}")

print("=" * 80)
print("COMPLETED")
print("=" * 80)