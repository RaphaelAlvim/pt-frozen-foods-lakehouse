# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: erp_salespersons
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

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

CLUSTER_COLUMNS = [
    "status_vendedor",
    "equipa",
    "senioridade"
]

REQUIRED_COLUMNS = [
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
]

print("=" * 80)
print("STARTING SILVER PROCESSING: erp_salespersons")
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
        F.sum(F.when(F.col("vendedor_id").isNull(), 1).otherwise(0)).alias("null_vendedor_id"),
        F.sum(F.when(F.col("nome_vendedor").isNull(), 1).otherwise(0)).alias("null_nome_vendedor"),
        F.sum(F.when(F.col("data_admissao").isNull(), 1).otherwise(0)).alias("null_data_admissao"),
        F.sum(F.when(F.col("status_vendedor").isNull(), 1).otherwise(0)).alias("null_status_vendedor")
    )
    .collect()[0]
)

print(f"Source row count:          {source_validation['row_count']:,}")
print(f"Null vendedor_id:          {source_validation['null_vendedor_id']:,}")
print(f"Null nome_vendedor:        {source_validation['null_nome_vendedor']:,}")
print(f"Null data_admissao:        {source_validation['null_data_admissao']:,}")
print(f"Null status_vendedor:      {source_validation['null_status_vendedor']:,}")

if source_validation["row_count"] == 0:
    raise ValueError("Source dataset is empty.")

source_null_failures = {
    "vendedor_id": source_validation["null_vendedor_id"],
    "nome_vendedor": source_validation["null_nome_vendedor"],
    "data_admissao": source_validation["null_data_admissao"],
    "status_vendedor": source_validation["null_status_vendedor"]
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
    NULLIF(TRIM(vendedor_id), '') AS vendedor_id,
    NULLIF(TRIM(nome_vendedor), '') AS nome_vendedor,

    CAST(idade AS INT) AS idade,

    NULLIF(TRIM(estado_civil), '') AS estado_civil,
    CAST(tem_filhos AS INT) AS tem_filhos,
    NULLIF(TRIM(cidade_morada), '') AS cidade_morada,

    COALESCE(
        TO_DATE(data_admissao, 'yyyy-MM-dd'),
        TO_DATE(data_admissao, 'dd-MM-yyyy'),
        TO_DATE(data_admissao, 'dd/MM/yyyy')
    ) AS data_admissao,

    COALESCE(
        TO_DATE(data_saida, 'yyyy-MM-dd'),
        TO_DATE(data_saida, 'dd-MM-yyyy'),
        TO_DATE(data_saida, 'dd/MM/yyyy')
    ) AS data_saida,

    NULLIF(TRIM(status_vendedor), '') AS status_vendedor,
    NULLIF(TRIM(equipa), '') AS equipa,
    NULLIF(TRIM(senioridade), '') AS senioridade,

    CAST(performance_score AS DECIMAL(18,4)) AS performance_score,

    NULLIF(TRIM(nota_interna), '') AS nota_interna,
    NULLIF(TRIM(telefone_extensao), '') AS telefone_extensao,

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
        F.countDistinct("vendedor_id").alias("distinct_vendedor_ids"),
        F.sum(F.when(F.col("vendedor_id").isNull(), 1).otherwise(0)).alias("null_vendedor_id"),
        F.sum(F.when(F.col("nome_vendedor").isNull(), 1).otherwise(0)).alias("null_nome_vendedor"),
        F.sum(F.when(F.col("data_admissao").isNull(), 1).otherwise(0)).alias("null_data_admissao"),
        F.sum(F.when(F.col("status_vendedor").isNull(), 1).otherwise(0)).alias("null_status_vendedor")
    )
    .collect()[0]
)

duplicate_vendedor_id_records = final["row_count"] - final["distinct_vendedor_ids"]

invalid_status_count = df_target.filter(
    ~F.col("status_vendedor").isin(VALID_STATUS_VALUES)
).count()

invalid_idade_count = df_target.filter(F.col("idade") <= 18).count()

invalid_tem_filhos_count = df_target.filter(
    ~F.col("tem_filhos").isin([0, 1])
).count()

invalid_performance_score_count = df_target.filter(
    F.col("performance_score") <= 0
).count()

invalid_ativo_com_data_saida_count = df_target.filter(
    (F.col("status_vendedor") == "Ativo") &
    F.col("data_saida").isNotNull()
).count()

invalid_inativo_sem_data_saida_count = df_target.filter(
    (F.col("status_vendedor") == "Inativo") &
    F.col("data_saida").isNull()
).count()

invalid_data_sequence_count = df_target.filter(
    F.col("data_saida").isNotNull() &
    F.col("data_admissao").isNotNull() &
    (F.col("data_saida") < F.col("data_admissao"))
).count()

print(f"Rows:                         {final['row_count']:,}")
print(f"Repeated vendedor_id records: {duplicate_vendedor_id_records:,}")
print(f"Null vendedor_id:             {final['null_vendedor_id']}")
print(f"Null nome_vendedor:           {final['null_nome_vendedor']}")
print(f"Null data_admissao:           {final['null_data_admissao']}")
print(f"Null status_vendedor:         {final['null_status_vendedor']}")

if final["row_count"] == 0:
    raise ValueError("Silver dataset is empty.")

critical_nulls = {
    "vendedor_id": final["null_vendedor_id"],
    "nome_vendedor": final["null_nome_vendedor"],
    "data_admissao": final["null_data_admissao"],
    "status_vendedor": final["null_status_vendedor"]
}

null_failures = {column: count for column, count in critical_nulls.items() if count > 0}

if null_failures:
    raise ValueError(f"Null values detected in critical columns: {null_failures}")

if invalid_status_count > 0:
    raise ValueError(f"Invalid status_vendedor values detected: {invalid_status_count}")

if invalid_idade_count > 0:
    raise ValueError(f"Invalid idade values detected: {invalid_idade_count}")

if invalid_tem_filhos_count > 0:
    raise ValueError(f"Invalid tem_filhos values detected: {invalid_tem_filhos_count}")

if invalid_performance_score_count > 0:
    raise ValueError(f"Invalid performance_score values detected: {invalid_performance_score_count}")

if invalid_ativo_com_data_saida_count > 0:
    raise ValueError(f"Active sellers with data_saida detected: {invalid_ativo_com_data_saida_count}")

if invalid_inativo_sem_data_saida_count > 0:
    raise ValueError(f"Inactive sellers without data_saida detected: {invalid_inativo_sem_data_saida_count}")

if invalid_data_sequence_count > 0:
    raise ValueError(f"Invalid date sequence detected: {invalid_data_sequence_count}")

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