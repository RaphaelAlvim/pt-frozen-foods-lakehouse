# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: erp_suppliers
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
DATASET = "erp_suppliers"

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


# ========================================
# 2. READ SOURCE DATA
# ========================================

df_source = spark.table(SOURCE_TABLE)
source_row_count = df_source.count()


# ========================================
# 3. CLEANING AND STANDARDIZATION
# ========================================

df_silver = df_source.select(
    "fornecedor_id",
    "nome_fornecedor",
    "pais",
    "status_fornecedor",
    "codigo_legacy",
    "ultima_sincronizacao",
    "load_date",
    "ingestion_timestamp",
    "source_file"
)

# STRING CLEANING
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

# REMOVE EXACT DUPLICATES
df_silver = df_silver.dropDuplicates()


# ========================================
# 4. DATA VALIDATION
# ========================================

silver_row_count = df_silver.count()

# Null checks
null_fornecedor_id = df_silver.filter(F.col("fornecedor_id").isNull()).count()
null_nome = df_silver.filter(F.col("nome_fornecedor").isNull()).count()
null_pais = df_silver.filter(F.col("pais").isNull()).count()

# Domain validation
invalid_status = df_silver.filter(
    ~F.col("status_fornecedor").isin(VALID_STATUS_VALUES)
).count()

# Business validation
future_sync = df_silver.filter(
    F.col("ultima_sincronizacao") > F.current_timestamp()
).count()


# ========================================
# 5. ERROR HANDLING
# ========================================

if null_fornecedor_id > 0:
    raise ValueError(f"[ERROR] fornecedor_id nulls: {null_fornecedor_id}")

if null_nome > 0:
    raise ValueError(f"[ERROR] nome_fornecedor nulls: {null_nome}")

if null_pais > 0:
    raise ValueError(f"[ERROR] pais nulls: {null_pais}")

if invalid_status > 0:
    raise ValueError(f"[ERROR] invalid status_fornecedor values: {invalid_status}")

if future_sync > 0:
    raise ValueError(f"[ERROR] ultima_sincronizacao in the future: {future_sync}")

if silver_row_count == 0:
    raise ValueError("[ERROR] Empty dataset")


# ========================================
# 6. WRITE TO DELTA
# ========================================

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(TARGET_PATH)
)


# ========================================
# 7. REGISTER TABLE
# ========================================

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

spark.sql(f"""
CREATE TABLE {TARGET_TABLE}
USING DELTA
LOCATION '{TARGET_PATH}'
""")


# ========================================
# 8. FINAL STATUS
# ========================================

print("===========================================")
print("SILVER PROCESS COMPLETED SUCCESSFULLY")
print(f"Dataset         : {DATASET}")
print(f"Source rows     : {source_row_count}")
print(f"Target rows     : {silver_row_count}")
print("===========================================")