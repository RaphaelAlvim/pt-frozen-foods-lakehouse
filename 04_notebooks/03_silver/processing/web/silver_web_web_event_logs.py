# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: web_event_logs
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "bronze"
TARGET_SCHEMA = "silver"

DOMAIN = "web"
DATASET = "web_event_logs"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
BRONZE_CONTAINER = "bronze"
SILVER_CONTAINER = "silver"

SOURCE_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{DATASET}"
TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"

SOURCE_PATH = f"abfss://{BRONZE_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

VALID_TIPO_EVENTO_VALUES = ["view", "search", "add_to_cart", "purchase"]
VALID_DISPOSITIVO_VALUES = ["Desktop", "Mobile", "Tablet"]
VALID_ORIGEM_TRAFEGO_VALUES = ["Orgânico", "Anúncio", "Direto", "Email", "Social"]
VALID_USER_AGENT_FAMILY_VALUES = ["Chrome", "Edge", "Firefox", "Safari"]


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
print(f"Source table      : {SOURCE_TABLE}")
print(f"Target table      : {TARGET_TABLE}")
print(f"Source path       : {SOURCE_PATH}")
print(f"Target path       : {TARGET_PATH}")
print(f"Valid event types : {VALID_TIPO_EVENTO_VALUES}")
print(f"Valid devices     : {VALID_DISPOSITIVO_VALUES}")
print(f"Valid traffic src : {VALID_ORIGEM_TRAFEGO_VALUES}")
print(f"Valid user agents : {VALID_USER_AGENT_FAMILY_VALUES}")
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
    "evento_id",
    "data_hora",
    "sessao_id",
    "cliente_id",
    "visitante_id",
    "produto_id",
    "tipo_evento",
    "valor_busca",
    "dispositivo",
    "origem_trafego",
    "user_agent_family",
    "tracking_batch_id",
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


# ========================================
# 5.1 BUSINESS-AWARE DEDUPLICATION
# ========================================

print("[INFO] Applying business-aware deduplication by evento_id...")

completeness_score = (
    F.when(F.col("cliente_id").isNotNull(), 1).otherwise(0) +
    F.when(F.col("visitante_id").isNotNull(), 1).otherwise(0) +
    F.when(F.col("produto_id").isNotNull(), 1).otherwise(0) +
    F.when(F.col("valor_busca").isNotNull(), 1).otherwise(0) +
    F.when(F.col("user_agent_family").isNotNull(), 1).otherwise(0) +
    F.when(F.col("tracking_batch_id").isNotNull(), 1).otherwise(0)
)

window_evento = Window.partitionBy("evento_id").orderBy(
    F.col("_completeness_score").desc(),
    F.col("ingestion_timestamp").desc(),
    F.col("tracking_batch_id").desc_nulls_last()
)

before_dedup_count = df_silver.count()

df_silver = (
    df_silver
    .withColumn("_completeness_score", completeness_score)
    .withColumn("_rn", F.row_number().over(window_evento))
    .filter(F.col("_rn") == 1)
    .drop("_completeness_score", "_rn")
)

after_dedup_count = df_silver.count()

print(f"[INFO] Business-aware deduplication applied: {before_dedup_count} -> {after_dedup_count}")


# ========================================
# 6. DATA QUALITY VALIDATION
# ========================================

silver_row_count = df_silver.count()

print(" ")
print("[INFO] Data quality validation completed")
print(f"[INFO] Silver row count : {silver_row_count}")

duplicate_evento_id_count = (
    df_silver.groupBy("evento_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"[INFO] Duplicate evento_id groups found after transformation: {duplicate_evento_id_count}")


# ========================================
# 7. CRITICAL BUSINESS VALIDATION
# ========================================

print("[INFO] Validating business rules...")

null_evento_id = df_silver.filter(F.col("evento_id").isNull()).count()
null_data_hora = df_silver.filter(F.col("data_hora").isNull()).count()
null_sessao_id = df_silver.filter(F.col("sessao_id").isNull()).count()
null_tipo_evento = df_silver.filter(F.col("tipo_evento").isNull()).count()
null_dispositivo = df_silver.filter(F.col("dispositivo").isNull()).count()
null_origem_trafego = df_silver.filter(F.col("origem_trafego").isNull()).count()
null_user_agent_family = df_silver.filter(F.col("user_agent_family").isNull()).count()

invalid_tipo_evento_count = df_silver.filter(
    ~F.col("tipo_evento").isin(VALID_TIPO_EVENTO_VALUES)
).count()

invalid_dispositivo_count = df_silver.filter(
    ~F.col("dispositivo").isin(VALID_DISPOSITIVO_VALUES)
).count()

invalid_origem_trafego_count = df_silver.filter(
    ~F.col("origem_trafego").isin(VALID_ORIGEM_TRAFEGO_VALUES)
).count()

invalid_user_agent_family_count = df_silver.filter(
    ~F.col("user_agent_family").isin(VALID_USER_AGENT_FAMILY_VALUES)
).count()

invalid_search_logic_count = df_silver.filter(
    (F.col("tipo_evento") == "search") &
    F.col("valor_busca").isNull()
).count()

invalid_non_search_logic_count = df_silver.filter(
    (F.col("tipo_evento") != "search") &
    F.col("valor_busca").isNotNull()
).count()

invalid_identity_logic_count = df_silver.filter(
    F.col("cliente_id").isNull() &
    F.col("visitante_id").isNull()
).count()

invalid_evento_id_count = df_silver.filter(
    F.col("evento_id") <= 0
).count()

if null_evento_id > 0:
    raise ValueError(f"[ERROR] evento_id contains {null_evento_id} null values")

if null_data_hora > 0:
    raise ValueError(f"[ERROR] data_hora contains {null_data_hora} null values")

if null_sessao_id > 0:
    raise ValueError(f"[ERROR] sessao_id contains {null_sessao_id} null values")

if null_tipo_evento > 0:
    raise ValueError(f"[ERROR] tipo_evento contains {null_tipo_evento} null values")

if null_dispositivo > 0:
    raise ValueError(f"[ERROR] dispositivo contains {null_dispositivo} null values")

if null_origem_trafego > 0:
    raise ValueError(f"[ERROR] origem_trafego contains {null_origem_trafego} null values")

if null_user_agent_family > 0:
    raise ValueError(f"[ERROR] user_agent_family contains {null_user_agent_family} null values")

if duplicate_evento_id_count > 0:
    raise ValueError(f"[ERROR] evento_id contains {duplicate_evento_id_count} duplicate groups after transformation")

if invalid_tipo_evento_count > 0:
    raise ValueError(f"[ERROR] tipo_evento contains {invalid_tipo_evento_count} invalid values")

if invalid_dispositivo_count > 0:
    raise ValueError(f"[ERROR] dispositivo contains {invalid_dispositivo_count} invalid values")

if invalid_origem_trafego_count > 0:
    raise ValueError(f"[ERROR] origem_trafego contains {invalid_origem_trafego_count} invalid values")

if invalid_user_agent_family_count > 0:
    raise ValueError(f"[ERROR] user_agent_family contains {invalid_user_agent_family_count} invalid values")

if invalid_search_logic_count > 0:
    raise ValueError(f"[ERROR] search events without valor_busca found in {invalid_search_logic_count} rows")

if invalid_non_search_logic_count > 0:
    raise ValueError(f"[ERROR] non-search events with valor_busca found in {invalid_non_search_logic_count} rows")

if invalid_identity_logic_count > 0:
    raise ValueError(f"[ERROR] events without cliente_id and visitante_id found in {invalid_identity_logic_count} rows")

if invalid_evento_id_count > 0:
    raise ValueError(f"[ERROR] evento_id contains {invalid_evento_id_count} non-positive values")

if silver_row_count == 0:
    raise ValueError("[ERROR] Silver dataset is empty after transformations")

print("[INFO] evento_id validation passed (no nulls)")
print("[INFO] data_hora validation passed (no nulls)")
print("[INFO] sessao_id validation passed (no nulls)")
print("[INFO] tipo_evento validation passed (no nulls)")
print("[INFO] dispositivo validation passed (no nulls)")
print("[INFO] origem_trafego validation passed (no nulls)")
print("[INFO] user_agent_family validation passed (no nulls)")
print("[INFO] tipo_evento domain validation passed")
print("[INFO] dispositivo domain validation passed")
print("[INFO] origem_trafego domain validation passed")
print("[INFO] user_agent_family domain validation passed")
print("[INFO] search event logic validation passed")
print("[INFO] identity logic validation passed")
print("[INFO] evento_id numeric validation passed")
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