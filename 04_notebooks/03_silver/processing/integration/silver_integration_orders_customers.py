# Databricks notebook source
# ========================================
# SILVER PROCESSING NOTEBOOK
# DATASET: silver_integration_orders_customers
# ========================================

from pyspark.sql import functions as F

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "silver"

DOMAIN = "integration"
DATASET = "silver_orders_customers"

STORAGE_ACCOUNT = "stptfrozenfoodsdevwe01"
SILVER_CONTAINER = "silver"

ORDERS_DATASET = "erp_orders"
CLIENTS_DATASET = "crm_clients"
SEGMENTATION_DATASET = "crm_segmentation"
STATUS_DATASET = "crm_status"

ORDERS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{ORDERS_DATASET}"
CLIENTS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{CLIENTS_DATASET}"
SEGMENTATION_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{SEGMENTATION_DATASET}"
STATUS_TABLE = f"{CATALOG}.{SOURCE_SCHEMA}.{STATUS_DATASET}"

TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.{DATASET}"
TARGET_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/{DOMAIN}/{DATASET}/"

CLUSTER_COLUMNS = [
    "data_pedido",
    "cliente_id"
]

ORDERS_REQUIRED_COLUMNS = [
    "pedido_id",
    "cliente_id",
    "data_pedido",
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
]

CLIENTS_REQUIRED_COLUMNS = [
    "cliente_id",
    "nome_cliente",
    "tipo_cliente",
    "nif",
    "data_registo",
    "cidade",
    "distrito",
    "canal_captacao",
    "score_atividade",
    "obs_comercial"
]

SEGMENTATION_REQUIRED_COLUMNS = [
    "cliente_id",
    "segmento",
    "potencial_valor",
    "cluster_comercial"
]

STATUS_REQUIRED_COLUMNS = [
    "cliente_id",
    "status_cliente",
    "data_status",
    "motivo_status"
]

print("=" * 80)
print("STARTING SILVER PROCESSING: silver_integration_orders_customers")
print("=" * 80)

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print("[INFO] Context setup completed successfully.")

# ========================================
# 1. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {ORDERS_TABLE}")
spark.sql(f"DESCRIBE TABLE {CLIENTS_TABLE}")
spark.sql(f"DESCRIBE TABLE {SEGMENTATION_TABLE}")
spark.sql(f"DESCRIBE TABLE {STATUS_TABLE}")

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")

# ========================================
# 2. SOURCE VALIDATION
# ========================================

print("[INFO] Validating source datasets...")

df_orders = spark.table(ORDERS_TABLE)
df_clients = spark.table(CLIENTS_TABLE)
df_segmentation = spark.table(SEGMENTATION_TABLE)
df_status = spark.table(STATUS_TABLE)

missing_orders_columns = [c for c in ORDERS_REQUIRED_COLUMNS if c not in df_orders.columns]
missing_clients_columns = [c for c in CLIENTS_REQUIRED_COLUMNS if c not in df_clients.columns]
missing_segmentation_columns = [c for c in SEGMENTATION_REQUIRED_COLUMNS if c not in df_segmentation.columns]
missing_status_columns = [c for c in STATUS_REQUIRED_COLUMNS if c not in df_status.columns]

missing_columns = {
    "orders": missing_orders_columns,
    "clients": missing_clients_columns,
    "segmentation": missing_segmentation_columns,
    "status": missing_status_columns
}

missing_columns = {table: cols for table, cols in missing_columns.items() if cols}

if missing_columns:
    raise ValueError(f"Missing required columns in source datasets: {missing_columns}")

orders_validation = (
    df_orders
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("pedido_id").alias("distinct_pedido_ids"),
        F.sum(F.when(F.col("pedido_id").isNull(), 1).otherwise(0)).alias("null_pedido_id"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id")
    )
    .collect()[0]
)

clients_validation = (
    df_clients
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("cliente_id").alias("distinct_cliente_ids"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id")
    )
    .collect()[0]
)

segmentation_validation = (
    df_segmentation
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("cliente_id").alias("distinct_cliente_ids"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id")
    )
    .collect()[0]
)

status_validation = (
    df_status
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("cliente_id").alias("distinct_cliente_ids"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id")
    )
    .collect()[0]
)

orders_repeated_pedido_id_records = orders_validation["row_count"] - orders_validation["distinct_pedido_ids"]

print(f"Orders row count:                       {orders_validation['row_count']:,}")
print(f"Orders repeated pedido_id records:      {orders_repeated_pedido_id_records:,}")
print(f"Orders null pedido_id:                  {orders_validation['null_pedido_id']:,}")
print(f"Orders null cliente_id:                 {orders_validation['null_cliente_id']:,}")

print(f"Clients row count:                      {clients_validation['row_count']:,}")
print(f"Clients null cliente_id:                {clients_validation['null_cliente_id']:,}")

print(f"Segmentation row count:                 {segmentation_validation['row_count']:,}")
print(f"Segmentation null cliente_id:           {segmentation_validation['null_cliente_id']:,}")

print(f"Status row count:                       {status_validation['row_count']:,}")
print(f"Status null cliente_id:                 {status_validation['null_cliente_id']:,}")

if orders_validation["row_count"] == 0:
    raise ValueError("Orders source dataset is empty.")

source_null_failures = {
    "orders.pedido_id": orders_validation["null_pedido_id"],
    "orders.cliente_id": orders_validation["null_cliente_id"]
}

source_null_failures = {column: count for column, count in source_null_failures.items() if count > 0}

if source_null_failures:
    raise ValueError(f"Null values detected in source critical columns: {source_null_failures}")

print("[INFO] Source validation completed successfully.")

# ========================================
# 3. CREATE SILVER INTEGRATION TABLE
# ========================================

print("[INFO] Creating Silver integration table using CTAS...")

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
WITH orders_scored AS (
    SELECT
        *,
        (
            CASE WHEN data_pedido IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN cliente_id IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN canal_id IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN vendedor_id IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN cidade_entrega IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN estado_pedido IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN prazo_entrega_dias IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN observacao_pedido IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN sistema_origem IS NOT NULL THEN 1 ELSE 0 END
        ) AS business_completeness_score
    FROM {ORDERS_TABLE}
),

orders_deduplicated AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY pedido_id
                ORDER BY
                    business_completeness_score DESC,
                    ingestion_timestamp DESC,
                    source_file DESC,
                    usuario_ultima_alteracao DESC
            ) AS row_num
        FROM orders_scored
    )
    WHERE row_num = 1
),

orders_selected AS (
    SELECT
        pedido_id,
        cliente_id,
        data_pedido,
        canal_id,
        vendedor_id,
        cidade_entrega,
        estado_pedido,
        prazo_entrega_dias,
        observacao_pedido,
        sistema_origem,
        usuario_ultima_alteracao,
        load_date AS order_load_date,
        ingestion_timestamp AS order_ingestion_timestamp,
        source_file AS order_source_file
    FROM orders_deduplicated
),

clients_selected AS (
    SELECT
        cliente_id,
        nome_cliente,
        tipo_cliente,
        nif,
        data_registo,
        cidade AS cliente_cidade,
        distrito,
        canal_captacao,
        score_atividade,
        obs_comercial
    FROM {CLIENTS_TABLE}
),

segmentation_selected AS (
    SELECT
        cliente_id,
        segmento,
        potencial_valor,
        cluster_comercial
    FROM {SEGMENTATION_TABLE}
),

status_selected AS (
    SELECT
        cliente_id,
        status_cliente,
        data_status,
        motivo_status
    FROM {STATUS_TABLE}
)

SELECT
    o.pedido_id,
    o.cliente_id,
    o.data_pedido,
    o.canal_id,
    o.vendedor_id,
    o.cidade_entrega,
    o.estado_pedido,
    o.prazo_entrega_dias,
    o.observacao_pedido,
    o.sistema_origem,
    o.usuario_ultima_alteracao,
    o.order_load_date,
    o.order_ingestion_timestamp,
    o.order_source_file,

    c.nome_cliente,
    c.tipo_cliente,
    c.nif,
    c.data_registo,
    c.cliente_cidade,
    c.distrito,
    c.canal_captacao,
    c.score_atividade,
    c.obs_comercial,

    s.segmento,
    s.potencial_valor,
    s.cluster_comercial,

    st.status_cliente,
    st.data_status,
    st.motivo_status

FROM orders_selected o
LEFT JOIN clients_selected c
    ON o.cliente_id = c.cliente_id
LEFT JOIN segmentation_selected s
    ON o.cliente_id = s.cliente_id
LEFT JOIN status_selected st
    ON o.cliente_id = st.cliente_id
""")

print("[INFO] Silver integration table created successfully.")

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

orders_expected = (
    df_orders
    .select("pedido_id")
    .distinct()
    .count()
)

final = (
    df_target
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("pedido_id").alias("distinct_pedido_ids"),
        F.sum(F.when(F.col("pedido_id").isNull(), 1).otherwise(0)).alias("null_pedido_id"),
        F.sum(F.when(F.col("cliente_id").isNull(), 1).otherwise(0)).alias("null_cliente_id")
    )
    .collect()[0]
)

duplicate_pedido_ids = final["row_count"] - final["distinct_pedido_ids"]

print(f"Expected row count after deduplication: {orders_expected:,}")
print(f"Output row count:                       {final['row_count']:,}")
print(f"Duplicate pedido_id records:            {duplicate_pedido_ids:,}")
print(f"Null pedido_id:                         {final['null_pedido_id']}")
print(f"Null cliente_id:                        {final['null_cliente_id']}")

if final["row_count"] == 0:
    raise ValueError("Silver integration dataset is empty.")

if final["row_count"] != orders_expected:
    raise ValueError(
        f"Row count mismatch detected. Expected: {orders_expected}, Found: {final['row_count']}"
    )

if duplicate_pedido_ids > 0:
    raise ValueError(f"Duplicate pedido_id detected in output dataset: {duplicate_pedido_ids}")

critical_nulls = {
    "pedido_id": final["null_pedido_id"],
    "cliente_id": final["null_cliente_id"]
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