# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_product
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "dimensions"
DATASET = "dim_product"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

SOURCE_PRODUCTS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.erp_products"
SOURCE_ORDER_ITEMS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.erp_order_items"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_COLUMNS = [
    "status_produto",
    "categoria"
]

REQUIRED_COLUMNS = [
    "produto_id",
    "produto_nome",
    "categoria",
    "marca",
    "peso_gramas",
    "preco_lista_base",
    "custo_base_unitario",
    "fornecedor_id",
    "data_lancamento",
    "data_fim",
    "status_produto",
    "popularidade_base",
    "sensibilidade_promocao",
    "fator_sazonal_proprio"
]

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_product")
print("=" * 80)

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print("[INFO] Context setup completed successfully.")

# ========================================
# 1. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {SOURCE_PRODUCTS_TABLE}")
spark.sql(f"DESCRIBE TABLE {SOURCE_ORDER_ITEMS_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")

# ========================================
# 2. SOURCE VALIDATION
# ========================================

print("[INFO] Validating source datasets...")

df_products = spark.table(SOURCE_PRODUCTS_TABLE)

missing_columns = [c for c in REQUIRED_COLUMNS if c not in df_products.columns]

if missing_columns:
    raise ValueError(f"Missing required columns in products source: {missing_columns}")

product_validation = (
    df_products
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("produto_id").alias("distinct_ids"),
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_id")
    )
    .collect()[0]
)

valid_product_keys_count = (
    spark.table(SOURCE_ORDER_ITEMS_TABLE)
    .filter(F.col("produto_id").isNotNull())
    .select("produto_id")
    .distinct()
    .count()
)

print(f"Products row count:             {product_validation['row_count']:,}")
print(f"Distinct produto_id:            {product_validation['distinct_ids']:,}")
print(f"Null produto_id in products:    {product_validation['null_id']:,}")
print(f"Valid product keys in sales:    {valid_product_keys_count:,}")

if product_validation["row_count"] != product_validation["distinct_ids"]:
    raise ValueError("produto_id is not unique in products source.")

if product_validation["null_id"] > 0:
    raise ValueError("Null produto_id detected in products source.")

print("[INFO] Source validation completed successfully.")

# ========================================
# 3. CREATE DIMENSION TABLE
# ========================================

print("[INFO] Creating Gold dimension table using CTAS...")

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
WITH valid_product_keys AS (
    SELECT DISTINCT produto_id
    FROM {SOURCE_ORDER_ITEMS_TABLE}
    WHERE produto_id IS NOT NULL
)

SELECT
    p.produto_id,
    p.produto_nome,
    p.categoria,
    p.marca,
    p.peso_gramas,
    p.preco_lista_base,
    p.custo_base_unitario,
    p.fornecedor_id,
    p.data_lancamento,
    p.data_fim,
    p.status_produto,
    p.popularidade_base,
    p.sensibilidade_promocao,
    p.fator_sazonal_proprio

FROM {SOURCE_PRODUCTS_TABLE} p
INNER JOIN valid_product_keys k
    ON p.produto_id = k.produto_id
""")

print("[INFO] Gold dimension table created successfully.")

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
        F.countDistinct("produto_id").alias("distinct_ids"),
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_produto_id"),
        F.sum(F.when(F.col("produto_nome").isNull(), 1).otherwise(0)).alias("null_produto_nome"),
        F.sum(F.when(F.col("categoria").isNull(), 1).otherwise(0)).alias("null_categoria"),
        F.sum(F.when(F.col("marca").isNull(), 1).otherwise(0)).alias("null_marca"),
        F.sum(F.when(F.col("fornecedor_id").isNull(), 1).otherwise(0)).alias("null_fornecedor_id"),
        F.sum(F.when(F.col("status_produto").isNull(), 1).otherwise(0)).alias("null_status_produto"),
        F.sum(F.when(F.col("peso_gramas").isNull(), 1).otherwise(0)).alias("null_peso_gramas")
    )
    .collect()[0]
)

duplicates = final["row_count"] - final["distinct_ids"]

print(f"Expected rows:           {valid_product_keys_count:,}")
print(f"Rows:                    {final['row_count']:,}")
print(f"Duplicates:              {duplicates:,}")
print(f"Null produto_id:         {final['null_produto_id']}")
print(f"Null produto_nome:       {final['null_produto_nome']}")
print(f"Null categoria:          {final['null_categoria']}")
print(f"Null marca:              {final['null_marca']}")
print(f"Null fornecedor_id:      {final['null_fornecedor_id']}")
print(f"Null status_produto:     {final['null_status_produto']}")
print(f"Null peso_gramas:        {final['null_peso_gramas']}")

if final["row_count"] != valid_product_keys_count:
    raise ValueError("Row count mismatch detected.")

if duplicates > 0:
    raise ValueError("Duplicate produto_id detected.")

critical_nulls = {
    "produto_id": final["null_produto_id"],
    "produto_nome": final["null_produto_nome"],
    "categoria": final["null_categoria"],
    "marca": final["null_marca"],
    "fornecedor_id": final["null_fornecedor_id"],
    "status_produto": final["null_status_produto"],
    "peso_gramas": final["null_peso_gramas"]
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