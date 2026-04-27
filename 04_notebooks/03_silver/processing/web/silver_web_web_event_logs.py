# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: web_event_logs
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

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

CLUSTER_COLUMNS = [
    "data_hora",
    "tipo_evento",
    "cliente_id"
]

REQUIRED_COLUMNS = [
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
]

print("=" * 80)
print("STARTING SILVER PROCESSING: web_event_logs")
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
        F.sum(F.when(F.col("evento_id").isNull(), 1).otherwise(0)).alias("null_evento_id"),
        F.sum(F.when(F.col("data_hora").isNull(), 1).otherwise(0)).alias("null_data_hora"),
        F.sum(F.when(F.col("sessao_id").isNull(), 1).otherwise(0)).alias("null_sessao_id"),
        F.sum(F.when(F.col("tipo_evento").isNull(), 1).otherwise(0)).alias("null_tipo_evento"),
        F.sum(F.when(F.col("dispositivo").isNull(), 1).otherwise(0)).alias("null_dispositivo"),
        F.sum(F.when(F.col("origem_trafego").isNull(), 1).otherwise(0)).alias("null_origem_trafego"),
        F.sum(F.when(F.col("user_agent_family").isNull(), 1).otherwise(0)).alias("null_user_agent_family")
    )
    .collect()[0]
)

print(f"Source row count:              {source_validation['row_count']:,}")
print(f"Null evento_id:                {source_validation['null_evento_id']:,}")
print(f"Null data_hora:                {source_validation['null_data_hora']:,}")
print(f"Null sessao_id:                {source_validation['null_sessao_id']:,}")
print(f"Null tipo_evento:              {source_validation['null_tipo_evento']:,}")
print(f"Null dispositivo:              {source_validation['null_dispositivo']:,}")
print(f"Null origem_trafego:           {source_validation['null_origem_trafego']:,}")
print(f"Null user_agent_family:        {source_validation['null_user_agent_family']:,}")

if source_validation["row_count"] == 0:
    raise ValueError("Source dataset is empty.")

source_null_failures = {
    "evento_id": source_validation["null_evento_id"],
    "data_hora": source_validation["null_data_hora"],
    "sessao_id": source_validation["null_sessao_id"],
    "tipo_evento": source_validation["null_tipo_evento"],
    "dispositivo": source_validation["null_dispositivo"],
    "origem_trafego": source_validation["null_origem_trafego"],
    "user_agent_family": source_validation["null_user_agent_family"]
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
        CAST(evento_id AS BIGINT) AS evento_id,

        COALESCE(
            TO_TIMESTAMP(data_hora, 'yyyy-MM-dd HH:mm:ss'),
            TO_TIMESTAMP(data_hora, 'yyyy/MM/dd HH:mm:ss'),
            TO_TIMESTAMP(data_hora, 'dd/MM/yyyy HH:mm:ss'),
            TO_TIMESTAMP(data_hora, 'dd-MM-yyyy HH:mm:ss')
        ) AS data_hora,

        NULLIF(TRIM(sessao_id), '') AS sessao_id,
        NULLIF(TRIM(cliente_id), '') AS cliente_id,
        NULLIF(TRIM(visitante_id), '') AS visitante_id,
        NULLIF(TRIM(produto_id), '') AS produto_id,
        NULLIF(TRIM(tipo_evento), '') AS tipo_evento,
        NULLIF(TRIM(valor_busca), '') AS valor_busca,
        NULLIF(TRIM(dispositivo), '') AS dispositivo,
        NULLIF(TRIM(origem_trafego), '') AS origem_trafego,
        NULLIF(TRIM(user_agent_family), '') AS user_agent_family,
        NULLIF(TRIM(tracking_batch_id), '') AS tracking_batch_id,

        load_date,
        ingestion_timestamp,
        source_file

    FROM {SOURCE_TABLE}
),

scored AS (
    SELECT
        *,
        (
            CASE WHEN cliente_id IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN visitante_id IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN produto_id IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN valor_busca IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN user_agent_family IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN tracking_batch_id IS NOT NULL THEN 1 ELSE 0 END
        ) AS completeness_score
    FROM standardized
),

deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY evento_id
                ORDER BY
                    completeness_score DESC,
                    ingestion_timestamp DESC,
                    tracking_batch_id DESC NULLS LAST
            ) AS rn
        FROM scored
    )
    WHERE rn = 1
)

SELECT
    evento_id,
    data_hora,
    sessao_id,
    cliente_id,
    visitante_id,
    produto_id,
    tipo_evento,
    valor_busca,
    dispositivo,
    origem_trafego,
    user_agent_family,
    tracking_batch_id,
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
        F.countDistinct("evento_id").alias("distinct_evento_ids"),
        F.sum(F.when(F.col("evento_id").isNull(), 1).otherwise(0)).alias("null_evento_id"),
        F.sum(F.when(F.col("data_hora").isNull(), 1).otherwise(0)).alias("null_data_hora"),
        F.sum(F.when(F.col("sessao_id").isNull(), 1).otherwise(0)).alias("null_sessao_id"),
        F.sum(F.when(F.col("tipo_evento").isNull(), 1).otherwise(0)).alias("null_tipo_evento"),
        F.sum(F.when(F.col("dispositivo").isNull(), 1).otherwise(0)).alias("null_dispositivo"),
        F.sum(F.when(F.col("origem_trafego").isNull(), 1).otherwise(0)).alias("null_origem_trafego"),
        F.sum(F.when(F.col("user_agent_family").isNull(), 1).otherwise(0)).alias("null_user_agent_family"),
        F.sum(F.when(~F.col("tipo_evento").isin(VALID_TIPO_EVENTO_VALUES), 1).otherwise(0)).alias("invalid_tipo_evento"),
        F.sum(F.when(~F.col("dispositivo").isin(VALID_DISPOSITIVO_VALUES), 1).otherwise(0)).alias("invalid_dispositivo"),
        F.sum(F.when(~F.col("origem_trafego").isin(VALID_ORIGEM_TRAFEGO_VALUES), 1).otherwise(0)).alias("invalid_origem_trafego"),
        F.sum(F.when(~F.col("user_agent_family").isin(VALID_USER_AGENT_FAMILY_VALUES), 1).otherwise(0)).alias("invalid_user_agent_family"),
        F.sum(F.when((F.col("tipo_evento") == "search") & F.col("valor_busca").isNull(), 1).otherwise(0)).alias("invalid_search_logic"),
        F.sum(F.when((F.col("tipo_evento") != "search") & F.col("valor_busca").isNotNull(), 1).otherwise(0)).alias("invalid_non_search_logic"),
        F.sum(F.when(F.col("cliente_id").isNull() & F.col("visitante_id").isNull(), 1).otherwise(0)).alias("invalid_identity_logic"),
        F.sum(F.when(F.col("evento_id") <= 0, 1).otherwise(0)).alias("invalid_evento_id")
    )
    .collect()[0]
)

duplicate_evento_id_records = final["row_count"] - final["distinct_evento_ids"]

print(f"Rows:                         {final['row_count']:,}")
print(f"Duplicate evento_id records:  {duplicate_evento_id_records:,}")
print(f"Null evento_id:               {final['null_evento_id']}")
print(f"Null data_hora:               {final['null_data_hora']}")
print(f"Null sessao_id:               {final['null_sessao_id']}")
print(f"Null tipo_evento:             {final['null_tipo_evento']}")
print(f"Null dispositivo:             {final['null_dispositivo']}")
print(f"Null origem_trafego:          {final['null_origem_trafego']}")
print(f"Null user_agent_family:       {final['null_user_agent_family']}")
print(f"Invalid tipo_evento:          {final['invalid_tipo_evento']}")
print(f"Invalid dispositivo:          {final['invalid_dispositivo']}")
print(f"Invalid origem_trafego:       {final['invalid_origem_trafego']}")
print(f"Invalid user_agent_family:    {final['invalid_user_agent_family']}")
print(f"Invalid search logic:         {final['invalid_search_logic']}")
print(f"Invalid non-search logic:     {final['invalid_non_search_logic']}")
print(f"Invalid identity logic:       {final['invalid_identity_logic']}")
print(f"Invalid evento_id:            {final['invalid_evento_id']}")

if final["row_count"] == 0:
    raise ValueError("Silver dataset is empty.")

critical_nulls = {
    "evento_id": final["null_evento_id"],
    "data_hora": final["null_data_hora"],
    "sessao_id": final["null_sessao_id"],
    "tipo_evento": final["null_tipo_evento"],
    "dispositivo": final["null_dispositivo"],
    "origem_trafego": final["null_origem_trafego"],
    "user_agent_family": final["null_user_agent_family"]
}

null_failures = {column: count for column, count in critical_nulls.items() if count > 0}

if null_failures:
    raise ValueError(f"Null values detected in critical columns: {null_failures}")

if duplicate_evento_id_records > 0:
    raise ValueError(f"Duplicate evento_id detected after transformation: {duplicate_evento_id_records}")

if final["invalid_tipo_evento"] > 0:
    raise ValueError(f"Invalid tipo_evento values detected: {final['invalid_tipo_evento']}")

if final["invalid_dispositivo"] > 0:
    raise ValueError(f"Invalid dispositivo values detected: {final['invalid_dispositivo']}")

if final["invalid_origem_trafego"] > 0:
    raise ValueError(f"Invalid origem_trafego values detected: {final['invalid_origem_trafego']}")

if final["invalid_user_agent_family"] > 0:
    raise ValueError(f"Invalid user_agent_family values detected: {final['invalid_user_agent_family']}")

if final["invalid_search_logic"] > 0:
    raise ValueError(f"Search events without valor_busca detected: {final['invalid_search_logic']}")

if final["invalid_non_search_logic"] > 0:
    raise ValueError(f"Non-search events with valor_busca detected: {final['invalid_non_search_logic']}")

if final["invalid_identity_logic"] > 0:
    raise ValueError(f"Events without cliente_id and visitante_id detected: {final['invalid_identity_logic']}")

if final["invalid_evento_id"] > 0:
    raise ValueError(f"Non-positive evento_id values detected: {final['invalid_evento_id']}")

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