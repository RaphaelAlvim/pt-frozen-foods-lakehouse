# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: erp_orders
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "erp"
DATASET = "erp_orders"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

# Key columns used only for duplicate monitoring.
# Use a list even when there is only one key column.
DUPLICATE_KEY_COLUMNS = ["pedido_id"]


# ========================================
# 1. CONTEXT SETUP
# ========================================

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print("Context configured successfully")
print(f"Catalog: {spark.catalog.currentCatalog()}")
print(f"Schema: {spark.catalog.currentDatabase()}")


# ========================================
# 2. CONFIGURATION SUMMARY
# ========================================

print(" ")
print("========== CONFIGURATION SUMMARY ==========")
print(f"Source table    : {SOURCE_TABLE}")
print(f"Target table    : {TARGET_TABLE}")
print(f"Source path     : {SOURCE_PATH}")
print(f"Target path     : {TARGET_PATH}")
print(f"Duplicate keys  : {DUPLICATE_KEY_COLUMNS}")
print("===========================================")


# ========================================
# 3. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {SOURCE_TABLE}")

print("[INFO] Checking source path access...")
source_items = dbutils.fs.ls(SOURCE_PATH)

print("[INFO] Checking target container access...")
target_items = dbutils.fs.ls(f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

if len(source_items) == 0:
    raise ValueError(f"[ERROR] No files found in source path: {SOURCE_PATH}")

print(" ")
print("Pre-checks completed successfully")
print(f"Source path accessible     : yes ({len(source_items)} items found)")
print(f"Target container access    : yes ({len(target_items)} items found)")


# ========================================
# 4. READ SOURCE DATA
# ========================================

df_source = spark.table(SOURCE_TABLE)
source_row_count = df_source.count()

print(" ")
print("Source data loaded successfully")
print(f"Source table     : {SOURCE_TABLE}")
print(f"Source row count : {source_row_count}")


# ========================================
# 5. CLEANING AND STANDARDIZATION
# ========================================

print(" ")
print("[INFO] Starting cleaning and standardization...")

df_silver = df_source.select(
    "pedido_id",
    "data_pedido",
    "cliente_id",
    "canal_id",
    "vendedor_id",
    "cidade_entrega",
    "estado_pedido",
    "prazo_entrega_dias",
    "observacao_pedido",
    "sistema_origem",
    "usuario_ultima_alteracao",
    "load_date",
    "ingestion_timestamp",
    "source_file"
)

print("[INFO] Selected relevant columns for Silver layer")

string_columns = [
    field.name
    for field in df_silver.schema.fields
    if isinstance(field.dataType, StringType)
]

for col_name in string_columns:
    df_silver = df_silver.withColumn(
        col_name,
        F.when(F.trim(F.col(col_name)) == "", None)
         .otherwise(F.trim(F.col(col_name)))
    )

print("[INFO] Applied base string standardization (trim + empty -> null)")

df_silver = df_silver.withColumn(
    "data_pedido",
    F.coalesce(
        F.to_date("data_pedido", "yyyy-MM-dd"),
        F.to_date("data_pedido", "dd/MM/yyyy"),
        F.to_date("data_pedido", "dd-MM-yyyy"),
        F.to_date("data_pedido", "yyyy/MM/dd")
    )
)

print("[INFO] Converted data_pedido to date using multiple input formats")

before_dedup_count = df_silver.count()

# Remove only exact duplicate rows.
# Do NOT deduplicate by pedido_id, because repeated pedido_id values may represent
# different business versions of the same order and not only technical duplication.
df_silver = df_silver.dropDuplicates()

after_dedup_count = df_silver.count()

print(f"[INFO] Exact-row deduplication applied: {before_dedup_count} -> {after_dedup_count}")


# ========================================
# 6. DATA QUALITY VALIDATION
# ========================================

silver_row_count = df_silver.count()

print(" ")
print("[INFO] Data quality validation completed")
print(f"[INFO] Silver row count : {silver_row_count}")

duplicate_key_count_after = (
    df_silver.groupBy(*DUPLICATE_KEY_COLUMNS)
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"[INFO] Remaining duplicate key groups after exact-row deduplication: {duplicate_key_count_after}")


# ========================================
# 7. CRITICAL DATA VALIDATION
# ========================================

print("[INFO] Validating critical business rules...")

null_pedido_id = df_silver.filter(F.col("pedido_id").isNull()).count()
null_data_pedido = df_silver.filter(F.col("data_pedido").isNull()).count()
null_cliente_id = df_silver.filter(F.col("cliente_id").isNull()).count()
null_canal_id = df_silver.filter(F.col("canal_id").isNull()).count()
null_estado_pedido = df_silver.filter(F.col("estado_pedido").isNull()).count()

if null_pedido_id > 0:
    raise ValueError(f"[ERROR] pedido_id contains {null_pedido_id} null values")

if null_data_pedido > 0:
    raise ValueError(f"[ERROR] data_pedido contains {null_data_pedido} null values after parsing")

if null_cliente_id > 0:
    raise ValueError(f"[ERROR] cliente_id contains {null_cliente_id} null values")

if null_canal_id > 0:
    raise ValueError(f"[ERROR] canal_id contains {null_canal_id} null values")

if null_estado_pedido > 0:
    raise ValueError(f"[ERROR] estado_pedido contains {null_estado_pedido} null values")

if silver_row_count == 0:
    raise ValueError("[ERROR] Silver dataset is empty after transformations")

print("[INFO] pedido_id validation passed (no nulls)")
print("[INFO] data_pedido validation passed (no nulls)")
print("[INFO] cliente_id validation passed (no nulls)")
print("[INFO] canal_id validation passed (no nulls)")
print("[INFO] estado_pedido validation passed (no nulls)")
print("[INFO] Silver dataset is not empty")


# ========================================
# 8. WRITE TO DELTA
# ========================================

print(" ")
print(f"[INFO] Writing Silver dataset to target path: {TARGET_PATH}")

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(TARGET_PATH)
)

print("[INFO] Silver dataset written successfully")


# ========================================
# 9. REGISTER TABLE
# ========================================

print(f"[INFO] Registering target table: {TARGET_TABLE}")

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

spark.sql(f"""
CREATE TABLE {TARGET_TABLE}
USING DELTA
LOCATION '{TARGET_PATH}'
""")

print("[INFO] Target table registered successfully")


# ========================================
# 10. FINAL STATUS
# ========================================

print(" ")
print("===========================================")
print("SILVER PROCESS COMPLETED SUCCESSFULLY")
print(f"Dataset         : {DATASET}")
print(f"Source table    : {SOURCE_TABLE}")
print(f"Target table    : {TARGET_TABLE}")
print(f"Target path     : {TARGET_PATH}")
print(f"Source row count: {source_row_count}")
print(f"Target row count: {silver_row_count}")
print("===========================================")