# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: erp_products
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "erp"
DATASET = "erp_products"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

VALID_STATUS_VALUES = ["Ativo", "Inativo"]

CLUSTER_COLUMNS = [
    "categoria",
    "marca",
    "status_produto"
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
    "fator_sazonal_proprio",
    "codigo_barra_legacy",
    "observacao_interna",
    "load_date",
    "ingestion_timestamp",
    "source_file"
]

print("=" * 80)
print("STARTING SILVER PROCESSING: erp_products")
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
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_produto_id"),
        F.sum(F.when(F.col("produto_nome").isNull(), 1).otherwise(0)).alias("null_produto_nome"),
        F.sum(F.when(F.col("categoria").isNull(), 1).otherwise(0)).alias("null_categoria"),
        F.sum(F.when(F.col("marca").isNull(), 1).otherwise(0)).alias("null_marca"),
        F.sum(F.when(F.col("fornecedor_id").isNull(), 1).otherwise(0)).alias("null_fornecedor_id"),
        F.sum(F.when(F.col("data_lancamento").isNull(), 1).otherwise(0)).alias("null_data_lancamento"),
        F.sum(F.when(F.col("status_produto").isNull(), 1).otherwise(0)).alias("null_status_produto")
    )
    .collect()[0]
)

print(f"Source row count:         {source_validation['row_count']:,}")
print(f"Null produto_id:          {source_validation['null_produto_id']:,}")
print(f"Null produto_nome:        {source_validation['null_produto_nome']:,}")
print(f"Null categoria:           {source_validation['null_categoria']:,}")
print(f"Null marca:               {source_validation['null_marca']:,}")
print(f"Null fornecedor_id:       {source_validation['null_fornecedor_id']:,}")
print(f"Null data_lancamento:     {source_validation['null_data_lancamento']:,}")
print(f"Null status_produto:      {source_validation['null_status_produto']:,}")

if source_validation["row_count"] == 0:
    raise ValueError("Source dataset is empty.")

source_null_failures = {
    "produto_id": source_validation["null_produto_id"],
    "produto_nome": source_validation["null_produto_nome"],
    "categoria": source_validation["null_categoria"],
    "marca": source_validation["null_marca"],
    "fornecedor_id": source_validation["null_fornecedor_id"],
    "data_lancamento": source_validation["null_data_lancamento"],
    "status_produto": source_validation["null_status_produto"]
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
SELECT DISTINCT
    NULLIF(TRIM(produto_id), '') AS produto_id,
    NULLIF(TRIM(produto_nome), '') AS produto_nome,
    NULLIF(TRIM(categoria), '') AS categoria,
    NULLIF(TRIM(marca), '') AS marca,

    CAST(peso_gramas AS INT) AS peso_gramas,
    CAST(preco_lista_base AS DECIMAL(18,4)) AS preco_lista_base,
    CAST(custo_base_unitario AS DECIMAL(18,4)) AS custo_base_unitario,

    NULLIF(TRIM(fornecedor_id), '') AS fornecedor_id,

    COALESCE(
        TO_DATE(data_lancamento, 'yyyy-MM-dd'),
        TO_DATE(data_lancamento, 'dd-MM-yyyy'),
        TO_DATE(data_lancamento, 'dd/MM/yyyy')
    ) AS data_lancamento,

    COALESCE(
        TO_DATE(data_fim, 'yyyy-MM-dd'),
        TO_DATE(data_fim, 'dd-MM-yyyy'),
        TO_DATE(data_fim, 'dd/MM/yyyy')
    ) AS data_fim,

    NULLIF(TRIM(status_produto), '') AS status_produto,

    popularidade_base,
    sensibilidade_promocao,
    fator_sazonal_proprio,

    CAST(codigo_barra_legacy AS STRING) AS codigo_barra_legacy,
    NULLIF(TRIM(observacao_interna), '') AS observacao_interna,

    load_date,
    ingestion_timestamp,
    source_file

FROM {SOURCE_TABLE}
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
        F.sum(F.when(F.col("produto_id").isNull(), 1).otherwise(0)).alias("null_produto_id"),
        F.sum(F.when(F.col("produto_nome").isNull(), 1).otherwise(0)).alias("null_produto_nome"),
        F.sum(F.when(F.col("categoria").isNull(), 1).otherwise(0)).alias("null_categoria"),
        F.sum(F.when(F.col("marca").isNull(), 1).otherwise(0)).alias("null_marca"),
        F.sum(F.when(F.col("fornecedor_id").isNull(), 1).otherwise(0)).alias("null_fornecedor_id"),
        F.sum(F.when(F.col("data_lancamento").isNull(), 1).otherwise(0)).alias("null_data_lancamento"),
        F.sum(F.when(F.col("status_produto").isNull(), 1).otherwise(0)).alias("null_status_produto")
    )
    .collect()[0]
)

invalid_status_count = df_target.filter(
    ~F.col("status_produto").isin(VALID_STATUS_VALUES)
).count()

invalid_preco_lista_count = df_target.filter(F.col("preco_lista_base") <= 0).count()
invalid_custo_base_count = df_target.filter(F.col("custo_base_unitario") <= 0).count()
invalid_preco_vs_custo_count = df_target.filter(F.col("preco_lista_base") < F.col("custo_base_unitario")).count()
invalid_peso_count = df_target.filter(F.col("peso_gramas") <= 0).count()

invalid_data_interval_count = df_target.filter(
    F.col("data_fim").isNotNull() &
    F.col("data_lancamento").isNotNull() &
    (F.col("data_fim") < F.col("data_lancamento"))
).count()

print(f"Rows: {final['row_count']:,}")

if final["row_count"] == 0:
    raise ValueError("Silver dataset is empty.")

critical_nulls = {
    "produto_id": final["null_produto_id"],
    "produto_nome": final["null_produto_nome"],
    "categoria": final["null_categoria"],
    "marca": final["null_marca"],
    "fornecedor_id": final["null_fornecedor_id"],
    "data_lancamento": final["null_data_lancamento"],
    "status_produto": final["null_status_produto"]
}

null_failures = {column: count for column, count in critical_nulls.items() if count > 0}

if null_failures:
    raise ValueError(f"Null values detected in critical columns: {null_failures}")

if invalid_status_count > 0:
    raise ValueError(f"Invalid status_produto values detected: {invalid_status_count}")

if invalid_preco_lista_count > 0:
    raise ValueError(f"Invalid preco_lista_base values detected: {invalid_preco_lista_count}")

if invalid_custo_base_count > 0:
    raise ValueError(f"Invalid custo_base_unitario values detected: {invalid_custo_base_count}")

if invalid_preco_vs_custo_count > 0:
    raise ValueError(f"Preco lower than cost detected: {invalid_preco_vs_custo_count}")

if invalid_peso_count > 0:
    raise ValueError(f"Invalid peso_gramas values detected: {invalid_peso_count}")

if invalid_data_interval_count > 0:
    raise ValueError(f"Invalid date interval detected: {invalid_data_interval_count}")

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