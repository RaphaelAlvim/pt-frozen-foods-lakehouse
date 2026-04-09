# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: erp_salespersons
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
DATASET = "erp_salespersons"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

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
    "vendedor_id",
    "nome_vendedor",
    "idade",
    "estado_civil",
    "tem_filhos",
    "cidade_morada",
    "data_admissao",
    "data_saida",
    "status_vendedor",
    "equipa",
    "senioridade",
    "performance_score",
    "nota_interna",
    "telefone_extensao",
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
    "data_admissao",
    F.coalesce(
        F.expr("try_to_date(data_admissao, 'yyyy-MM-dd')"),
        F.expr("try_to_date(data_admissao, 'dd-MM-yyyy')"),
        F.expr("try_to_date(data_admissao, 'dd/MM/yyyy')")
    )
)

df_silver = df_silver.withColumn(
    "data_saida",
    F.coalesce(
        F.expr("try_to_date(data_saida, 'yyyy-MM-dd')"),
        F.expr("try_to_date(data_saida, 'dd-MM-yyyy')"),
        F.expr("try_to_date(data_saida, 'dd/MM/yyyy')")
    )
)

print("[INFO] Converted date columns using multiple input formats with safe parsing")

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

duplicate_vendedor_id_count = (
    df_silver.groupBy("vendedor_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"[INFO] Duplicate vendedor_id groups found after transformation: {duplicate_vendedor_id_count}")


# ========================================
# 7. CRITICAL DATA VALIDATION
# ========================================

print("[INFO] Validating critical business rules...")

null_vendedor_id = df_silver.filter(F.col("vendedor_id").isNull()).count()
null_nome_vendedor = df_silver.filter(F.col("nome_vendedor").isNull()).count()
null_data_admissao = df_silver.filter(F.col("data_admissao").isNull()).count()
null_status_vendedor = df_silver.filter(F.col("status_vendedor").isNull()).count()

invalid_status_count = df_silver.filter(
    ~F.col("status_vendedor").isin(VALID_STATUS_VALUES)
).count()

invalid_idade_count = df_silver.filter(
    F.col("idade") <= 18
).count()

invalid_tem_filhos_count = df_silver.filter(
    ~F.col("tem_filhos").isin([0, 1])
).count()

invalid_performance_score_count = df_silver.filter(
    F.col("performance_score") <= 0
).count()

invalid_ativo_com_data_saida_count = df_silver.filter(
    (F.col("status_vendedor") == "Ativo") &
    F.col("data_saida").isNotNull()
).count()

invalid_inativo_sem_data_saida_count = df_silver.filter(
    (F.col("status_vendedor") == "Inativo") &
    F.col("data_saida").isNull()
).count()

invalid_data_sequence_count = df_silver.filter(
    F.col("data_saida").isNotNull() &
    F.col("data_admissao").isNotNull() &
    (F.col("data_saida") < F.col("data_admissao"))
).count()

if null_vendedor_id > 0:
    raise ValueError(f"[ERROR] vendedor_id contains {null_vendedor_id} null values")

if null_nome_vendedor > 0:
    raise ValueError(f"[ERROR] nome_vendedor contains {null_nome_vendedor} null values")

if null_data_admissao > 0:
    raise ValueError(f"[ERROR] data_admissao contains {null_data_admissao} null values after parsing")

if null_status_vendedor > 0:
    raise ValueError(f"[ERROR] status_vendedor contains {null_status_vendedor} null values")

if invalid_status_count > 0:
    raise ValueError(f"[ERROR] status_vendedor contains {invalid_status_count} invalid values")

if invalid_idade_count > 0:
    raise ValueError(f"[ERROR] idade contains {invalid_idade_count} invalid values (<= 18)")

if invalid_tem_filhos_count > 0:
    raise ValueError(f"[ERROR] tem_filhos contains {invalid_tem_filhos_count} invalid values")

if invalid_performance_score_count > 0:
    raise ValueError(f"[ERROR] performance_score contains {invalid_performance_score_count} non-positive values")

if invalid_ativo_com_data_saida_count > 0:
    raise ValueError(f"[ERROR] active sellers with data_saida found in {invalid_ativo_com_data_saida_count} rows")

if invalid_inativo_sem_data_saida_count > 0:
    raise ValueError(f"[ERROR] inactive sellers without data_saida found in {invalid_inativo_sem_data_saida_count} rows")

if invalid_data_sequence_count > 0:
    raise ValueError(f"[ERROR] data_saida earlier than data_admissao found in {invalid_data_sequence_count} rows")

if silver_row_count == 0:
    raise ValueError("[ERROR] Silver dataset is empty after transformations")

print("[INFO] vendedor_id validation passed (no nulls)")
print("[INFO] nome_vendedor validation passed (no nulls)")
print("[INFO] data_admissao validation passed (no nulls)")
print("[INFO] status_vendedor validation passed (no nulls)")
print("[INFO] status_vendedor domain validation passed")
print("[INFO] idade validation passed")
print("[INFO] tem_filhos validation passed")
print("[INFO] performance_score validation passed")
print("[INFO] status versus data_saida validation passed")
print("[INFO] date sequence validation passed")
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