# Databricks notebook source
# ========================================
# GOLD PROCESSING NOTEBOOK
# DATASET: dim_product
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "gold"

DOMAIN = "dimensions"
DATASET = "dim_product"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
GOLD_CONTAINER = "gold"

PRODUCTS_DATASET = "erp_products"
ORDER_ITEMS_DATASET = "erp_order_items"

PRODUCTS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{PRODUCTS_DATASET}"
ORDER_ITEMS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{ORDER_ITEMS_DATASET}"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_KEYS = ["status_produto", "categoria"]

AUTO_OPTIMIZE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}


# ========================================
# 1. CONTEXT SETUP
# ========================================

print("=" * 80)
print("STARTING GOLD PROCESSING: dim_product")
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
print(f"Products table:                  {PRODUCTS_TABLE}")
print(f"Order items table:               {ORDER_ITEMS_TABLE}")
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
spark.sql(f"DESCRIBE TABLE {PRODUCTS_TABLE}")
spark.sql(f"DESCRIBE TABLE {ORDER_ITEMS_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{GOLD_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")


# ========================================
# 4. READ SOURCE DATA
# ========================================

df_products = spark.table(PRODUCTS_TABLE)
df_order_items = spark.table(ORDER_ITEMS_TABLE)

print("[INFO] Source data loaded successfully.")
print(f"[INFO] Products row count:                   {df_products.count():,}")
print(f"[INFO] Order items row count:                {df_order_items.count():,}")


# ========================================
# 5. SOURCE VALIDATION
# ========================================

print("[INFO] Validating source datasets...")

raw_products_count = df_products.count()
distinct_product_ids = df_products.select("produto_id").distinct().count()
null_product_ids_products = df_products.filter(F.col("produto_id").isNull()).count()
null_product_ids_order_items = df_order_items.filter(F.col("produto_id").isNull()).count()

if distinct_product_ids != raw_products_count:
    raise ValueError(
        f"produto_id is not unique in products source. Distinct: {distinct_product_ids}, Rows: {raw_products_count}"
    )

if null_product_ids_products > 0:
    raise ValueError(f"Null produto_id detected in products source dataset: {null_product_ids_products}")

print(f"[INFO] produto_id uniqueness validated:      {distinct_product_ids:,}")
print(f"[INFO] Null produto_id in products:          {null_product_ids_products:,}")
print(f"[INFO] Null produto_id in order items:       {null_product_ids_order_items:,}")

required_products_columns = [
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

missing_products_columns = [c for c in required_products_columns if c not in df_products.columns]

if missing_products_columns:
    raise ValueError(f"Missing required columns in products source: {missing_products_columns}")

print("[INFO] Source validation completed successfully.")


# ========================================
# 6. FILTER DIMENSION CANDIDATES
# ========================================

print("[INFO] Building valid product key set from order items...")
print("[INFO] Coverage was validated in the Gold exploratory notebook.")

df_valid_product_keys = (
    df_order_items
    .filter(F.col("produto_id").isNotNull())
    .select("produto_id")
    .distinct()
)

valid_product_keys_count = df_valid_product_keys.count()

print(f"[INFO] Distinct valid product keys in sales: {valid_product_keys_count:,}")
print("[INFO] Dimension candidate filtering completed successfully.")


# ========================================
# 7. BUILD DIMENSION DATASET
# ========================================

print("[INFO] Building Gold dimension dataset with explicit column selection...")

df_dim_product = (
    df_products.alias("p")
    .join(
        F.broadcast(df_valid_product_keys).alias("k"),
        on="produto_id",
        how="inner"
    )
    .select(
        F.col("produto_id"),
        F.col("produto_nome"),
        F.col("categoria"),
        F.col("marca"),
        F.col("peso_gramas"),
        F.col("preco_lista_base"),
        F.col("custo_base_unitario"),
        F.col("fornecedor_id"),
        F.col("data_lancamento"),
        F.col("data_fim"),
        F.col("status_produto"),
        F.col("popularidade_base"),
        F.col("sensibilidade_promocao"),
        F.col("fator_sazonal_proprio")
    )
    .dropDuplicates(["produto_id"])
)

print("[INFO] Gold dimension dataset built successfully.")
print(f"[INFO] Row count after build:                {df_dim_product.count():,}")


# ========================================
# 8. OUTPUT VALIDATION
# ========================================

print("[INFO] Validating Gold dimension output...")

final_row_count = df_dim_product.count()
expected_row_count = valid_product_keys_count

duplicate_product_ids = (
    df_dim_product
    .groupBy("produto_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_produto_id_final = df_dim_product.filter(F.col("produto_id").isNull()).count()
null_produto_nome_final = df_dim_product.filter(F.col("produto_nome").isNull()).count()
null_categoria_final = df_dim_product.filter(F.col("categoria").isNull()).count()
null_marca_final = df_dim_product.filter(F.col("marca").isNull()).count()
null_fornecedor_id_final = df_dim_product.filter(F.col("fornecedor_id").isNull()).count()
null_status_produto_final = df_dim_product.filter(F.col("status_produto").isNull()).count()
null_peso_gramas_final = df_dim_product.filter(F.col("peso_gramas").isNull()).count()

print(f"[INFO] Expected row count:                   {expected_row_count:,}")
print(f"[INFO] Output row count:                     {final_row_count:,}")
print(f"[INFO] Duplicate produto_id count:          {duplicate_product_ids:,}")
print(f"[INFO] Null produto_id count:               {null_produto_id_final:,}")
print(f"[INFO] Null produto_nome count:             {null_produto_nome_final:,}")
print(f"[INFO] Null categoria count:                {null_categoria_final:,}")
print(f"[INFO] Null marca count:                    {null_marca_final:,}")
print(f"[INFO] Null fornecedor_id count:            {null_fornecedor_id_final:,}")
print(f"[INFO] Null status_produto count:           {null_status_produto_final:,}")
print(f"[INFO] Null peso_gramas count:              {null_peso_gramas_final:,}")

if final_row_count != expected_row_count:
    raise ValueError(
        f"Row count mismatch detected. Expected: {expected_row_count}, Got: {final_row_count}"
    )

if duplicate_product_ids > 0:
    raise ValueError(f"Duplicate produto_id detected in output dataset: {duplicate_product_ids}")

if null_produto_id_final > 0:
    raise ValueError(f"Null produto_id detected in output dataset: {null_produto_id_final}")

if null_produto_nome_final > 0:
    raise ValueError(f"Null produto_nome detected in output dataset: {null_produto_nome_final}")

if null_categoria_final > 0:
    raise ValueError(f"Null categoria detected in output dataset: {null_categoria_final}")

if null_marca_final > 0:
    raise ValueError(f"Null marca detected in output dataset: {null_marca_final}")

if null_fornecedor_id_final > 0:
    raise ValueError(f"Null fornecedor_id detected in output dataset: {null_fornecedor_id_final}")

if null_status_produto_final > 0:
    raise ValueError(f"Null status_produto detected in output dataset: {null_status_produto_final}")

if null_peso_gramas_final > 0:
    raise ValueError(f"Null peso_gramas detected in output dataset: {null_peso_gramas_final}")

print("[INFO] Output validation completed successfully.")


# ========================================
# 9. WRITE DELTA TABLE
# ========================================

print("[INFO] Writing Gold dimension table to Delta format...")

(
    df_dim_product.write
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