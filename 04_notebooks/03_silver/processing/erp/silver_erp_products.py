# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: erp_products
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "erp"
DATASET = "erp_products"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

# COMMAND ----------

# ========================================
# 1. CONTEXT SETUP
# ========================================

# Set catalog
spark.sql(f"USE CATALOG {CATALOG}")

# Ensure target schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")

# Set working schema (optional but keeps consistency)
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

# COMMAND ----------

# ========================================
# 2. CONFIGURATION SUMMARY
# ========================================

print("=" * 80)
print("SILVER PROCESSING NOTEBOOK CONFIGURATION")
print("=" * 80)
print(f"Catalog:        {CATALOG}")
print(f"Source schema:  {SOURCE_SCHEMA}")
print(f"Target schema:  {TARGET_SCHEMA}")
print(f"Domain:         {DOMAIN}")
print(f"Dataset:        {DATASET}")
print(f"Source table:   {SOURCE_TABLE}")
print(f"Target table:   {TARGET_TABLE}")
print(f"Source path:    {SOURCE_PATH}")
print(f"Target path:    {TARGET_PATH}")
print("=" * 80)

# COMMAND ----------

# ========================================
# 3. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {SOURCE_TABLE}")

print("[INFO] Checking source path access...")
dbutils.fs.ls(SOURCE_PATH)

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")

# COMMAND ----------

# ========================================
# 4. READ SOURCE DATA
# ========================================

df_source = spark.table(SOURCE_TABLE)

print("[INFO] Source data loaded successfully.")
print(f"[INFO] Source table: {SOURCE_TABLE}")

display(df_source.limit(20))

# COMMAND ----------

# ========================================
# 5. INITIAL INSPECTION
# ========================================

print("[INFO] Source schema:")
df_source.printSchema()

print("[INFO] Source columns:")
print(df_source.columns)

source_row_count = df_source.count()
print(f"[INFO] Source row count: {source_row_count}")

# COMMAND ----------

# ========================================
# 6. INITIAL PROFILING CHECKS
# ========================================

from pyspark.sql import functions as F

# Duplicate inspection key columns
# Use a list even for a single key column.
# Examples:
# ["pedido_id"]
# ["pedido_id", "produto_id"]
DUPLICATE_KEY_COLUMNS = ["produto_id"]

# Technical columns that should be excluded from the business preview
TECHNICAL_COLUMNS = ["_rescued_data", "ingestion_timestamp", "source_file"]

# Set to True only if the dataset contains _rescued_data and you want to inspect it
CHECK_RESCUED_DATA = "_rescued_data" in df_source.columns

# Business preview columns:
# automatically include all columns except technical ones
BUSINESS_PREVIEW_COLUMNS = [
    c for c in df_source.columns
    if c not in TECHNICAL_COLUMNS
]

print("[INFO] Null check by column...")

null_counts_df = df_source.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_source.columns
])

null_counts_row = null_counts_df.collect()[0].asDict()

null_stats_data = [
    (
        column_name,
        null_count,
        round((null_count / source_row_count) * 100, 2) if source_row_count > 0 else 0.0
    )
    for column_name, null_count in null_counts_row.items()
]

df_null_stats = spark.createDataFrame(
    null_stats_data,
    ["column_name", "null_count", "null_percentage"]
)

display(
    df_null_stats.orderBy(F.col("null_percentage").desc(), F.col("null_count").desc())
)

if CHECK_RESCUED_DATA:
    print("[INFO] Checking rescued data records...")
    display(
        df_source.filter(F.col("_rescued_data").isNotNull())
    )

print(f"[INFO] Checking duplicate values using key columns: {DUPLICATE_KEY_COLUMNS}...")
display(
    df_source.groupBy(*DUPLICATE_KEY_COLUMNS)
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.col("count").desc())
)

print("[INFO] Preview of business columns...")
display(
    df_source.select(*BUSINESS_PREVIEW_COLUMNS).limit(20)
)

# COMMAND ----------

# ========================================
# 7. DUPLICATE RECORD INSPECTION
# ========================================

# Duplicate inspection key columns
# Use a list even for a single key column.
# Examples:
# ["item_pedido_id"]
# ["pedido_id", "produto_id"]
DUPLICATE_KEY_COLUMNS = ["produto_id"]

# Optional column to monitor nulls inside duplicated rows
# Set to None if not needed
DUPLICATE_NULL_CHECK_COLUMN = None

print(f"[INFO] Inspecting duplicate records using key columns: {DUPLICATE_KEY_COLUMNS}")

duplicate_key_df = (
    df_source.groupBy(*DUPLICATE_KEY_COLUMNS)
    .count()
    .filter(F.col("count") > 1)
    .select(*DUPLICATE_KEY_COLUMNS)
)

duplicate_group_count = duplicate_key_df.count()

duplicate_rows_df = df_source.join(
    duplicate_key_df,
    on=DUPLICATE_KEY_COLUMNS,
    how="inner"
)

duplicate_row_count = duplicate_rows_df.count()
distinct_duplicate_records_count = duplicate_rows_df.dropDuplicates().count()
exact_duplicate_row_count = duplicate_row_count - distinct_duplicate_records_count

print("=" * 80)
print("DUPLICATE INSPECTION SUMMARY")
print("=" * 80)
print(f"Duplicate key columns                : {DUPLICATE_KEY_COLUMNS}")
print(f"Duplicate groups                     : {duplicate_group_count}")
print(f"Duplicate rows (total)               : {duplicate_row_count}")
print(f"Distinct duplicate records           : {distinct_duplicate_records_count}")
print(f"Exact duplicate rows detected        : {exact_duplicate_row_count}")

if DUPLICATE_NULL_CHECK_COLUMN:
    null_check_count = duplicate_rows_df.filter(
        F.col(DUPLICATE_NULL_CHECK_COLUMN).isNull()
    ).count()
    print(f"Duplicate rows with {DUPLICATE_NULL_CHECK_COLUMN} null : {null_check_count}")

print("=" * 80)

sample_duplicate_keys = duplicate_key_df.limit(10).collect()
print(f"Sample duplicate key values          : {sample_duplicate_keys}")

# COMMAND ----------

# ========================================
# 8. DUPLICATE CONFLICT ANALYSIS
# ========================================

DUPLICATE_KEY_COLUMNS = ["produto_id"]

print(f"[INFO] Analysing duplicate conflicts using key columns: {DUPLICATE_KEY_COLUMNS}")

duplicate_key_df = (
    df_source.groupBy(*DUPLICATE_KEY_COLUMNS)
    .count()
    .filter(F.col("count") > 1)
    .select(*DUPLICATE_KEY_COLUMNS)
)

duplicate_rows_df = df_source.join(
    duplicate_key_df,
    on=DUPLICATE_KEY_COLUMNS,
    how="inner"
)

summary_exprs = []
for col_name in BUSINESS_PREVIEW_COLUMNS:
    summary_exprs.append(F.countDistinct(F.col(col_name)).alias(f"{col_name}__distinct_count"))

duplicate_conflict_summary = (
    duplicate_rows_df.groupBy(*DUPLICATE_KEY_COLUMNS)
    .agg(*summary_exprs)
)

conflict_condition = None
for col_name in BUSINESS_PREVIEW_COLUMNS:
    current_condition = F.col(f"{col_name}__distinct_count") > 1
    conflict_condition = current_condition if conflict_condition is None else (conflict_condition | current_condition)

conflicting_keys_df = duplicate_conflict_summary.filter(conflict_condition)

conflicting_key_count = conflicting_keys_df.count()
total_duplicate_key_count = duplicate_key_df.count()

print("=" * 80)
print("DUPLICATE CONFLICT ANALYSIS SUMMARY")
print("=" * 80)
print(f"Duplicate key groups analysed        : {total_duplicate_key_count}")
print(f"Conflicting duplicate key groups     : {conflicting_key_count}")
print(f"Non-conflicting duplicate key groups : {total_duplicate_key_count - conflicting_key_count}")
print("=" * 80)

display(conflicting_keys_df.limit(20))

# COMMAND ----------

# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: erp_products
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
DATASET = "erp_products"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

# Valid status values for business validation
VALID_STATUS_VALUES = ["Ativo", "Inativo"]


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
print(f"Valid status    : {VALID_STATUS_VALUES}")
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
    "codigo_barra_legacy",
    F.col("codigo_barra_legacy").cast("string")
)

print("[INFO] Cast applied: codigo_barra_legacy -> string")

df_silver = df_silver.withColumn(
    "data_lancamento",
    F.coalesce(
        F.to_date("data_lancamento", "yyyy-MM-dd"),
        F.to_date("data_lancamento", "dd-MM-yyyy"),
        F.to_date("data_lancamento", "dd/MM/yyyy")
    )
)

df_silver = df_silver.withColumn(
    "data_fim",
    F.coalesce(
        F.to_date("data_fim", "yyyy-MM-dd"),
        F.to_date("data_fim", "dd-MM-yyyy"),
        F.to_date("data_fim", "dd/MM/yyyy")
    )
)

print("[INFO] Converted date columns using multiple input formats")

before_dedup_count = df_silver.count()
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

duplicate_produto_id_count = (
    df_silver.groupBy("produto_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"[INFO] Duplicate produto_id groups found after transformation: {duplicate_produto_id_count}")


# ========================================
# 7. CRITICAL DATA VALIDATION
# ========================================

print("[INFO] Validating critical business rules...")

null_produto_id = df_silver.filter(F.col("produto_id").isNull()).count()
null_produto_nome = df_silver.filter(F.col("produto_nome").isNull()).count()
null_categoria = df_silver.filter(F.col("categoria").isNull()).count()
null_marca = df_silver.filter(F.col("marca").isNull()).count()
null_fornecedor_id = df_silver.filter(F.col("fornecedor_id").isNull()).count()
null_data_lancamento = df_silver.filter(F.col("data_lancamento").isNull()).count()
null_status_produto = df_silver.filter(F.col("status_produto").isNull()).count()

invalid_status_count = df_silver.filter(
    ~F.col("status_produto").isin(VALID_STATUS_VALUES)
).count()

invalid_preco_lista_count = df_silver.filter(
    F.col("preco_lista_base") <= 0
).count()

invalid_custo_base_count = df_silver.filter(
    F.col("custo_base_unitario") <= 0
).count()

invalid_preco_vs_custo_count = df_silver.filter(
    F.col("preco_lista_base") < F.col("custo_base_unitario")
).count()

invalid_peso_count = df_silver.filter(
    F.col("peso_gramas") <= 0
).count()

invalid_data_interval_count = df_silver.filter(
    F.col("data_fim").isNotNull() &
    F.col("data_lancamento").isNotNull() &
    (F.col("data_fim") < F.col("data_lancamento"))
).count()

if null_produto_id > 0:
    raise ValueError(f"[ERROR] produto_id contains {null_produto_id} null values")

if null_produto_nome > 0:
    raise ValueError(f"[ERROR] produto_nome contains {null_produto_nome} null values")

if null_categoria > 0:
    raise ValueError(f"[ERROR] categoria contains {null_categoria} null values")

if null_marca > 0:
    raise ValueError(f"[ERROR] marca contains {null_marca} null values")

if null_fornecedor_id > 0:
    raise ValueError(f"[ERROR] fornecedor_id contains {null_fornecedor_id} null values")

if null_data_lancamento > 0:
    raise ValueError(f"[ERROR] data_lancamento contains {null_data_lancamento} null values after parsing")

if null_status_produto > 0:
    raise ValueError(f"[ERROR] status_produto contains {null_status_produto} null values")

if invalid_status_count > 0:
    raise ValueError(f"[ERROR] status_produto contains {invalid_status_count} invalid values")

if invalid_preco_lista_count > 0:
    raise ValueError(f"[ERROR] preco_lista_base contains {invalid_preco_lista_count} non-positive values")

if invalid_custo_base_count > 0:
    raise ValueError(f"[ERROR] custo_base_unitario contains {invalid_custo_base_count} non-positive values")

if invalid_preco_vs_custo_count > 0:
    raise ValueError(f"[ERROR] preco_lista_base is lower than custo_base_unitario in {invalid_preco_vs_custo_count} rows")

if invalid_peso_count > 0:
    raise ValueError(f"[ERROR] peso_gramas contains {invalid_peso_count} non-positive values")

if invalid_data_interval_count > 0:
    raise ValueError(f"[ERROR] data_fim is earlier than data_lancamento in {invalid_data_interval_count} rows")

if silver_row_count == 0:
    raise ValueError("[ERROR] Silver dataset is empty after transformations")

print("[INFO] produto_id validation passed (no nulls)")
print("[INFO] produto_nome validation passed (no nulls)")
print("[INFO] categoria validation passed (no nulls)")
print("[INFO] marca validation passed (no nulls)")
print("[INFO] fornecedor_id validation passed (no nulls)")
print("[INFO] data_lancamento validation passed (no nulls)")
print("[INFO] status_produto validation passed (no nulls)")
print("[INFO] status_produto domain validation passed")
print("[INFO] preco_lista_base validation passed")
print("[INFO] custo_base_unitario validation passed")
print("[INFO] preco versus custo validation passed")
print("[INFO] peso_gramas validation passed")
print("[INFO] date interval validation passed")
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