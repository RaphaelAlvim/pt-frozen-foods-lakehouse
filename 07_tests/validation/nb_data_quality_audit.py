# Databricks notebook source
# ========================================
# DATA QUALITY AUDIT NOTEBOOK
# ALL LAYERS
# ========================================

from pyspark.sql import functions as F
from datetime import datetime

# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

FAIL_ON_ERRORS = True

AUDIT_STARTED_AT = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

audit_results = []

print("=" * 80)
print("STARTING DATA QUALITY AUDIT")
print("=" * 80)
print(f"Catalog:      {CATALOG}")
print(f"Started at:   {AUDIT_STARTED_AT}")
print("=" * 80)


# ========================================
# 1. HELPERS
# ========================================

def add_result(layer, table, check_name, status, details):
    audit_results.append({
        "layer": layer,
        "table": table,
        "check_name": check_name,
        "status": status,
        "details": str(details)
    })

    print(f"[{status}] {layer}.{table} | {check_name} | {details}")


def table_exists(full_table_name):
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False


def get_row_count(full_table_name):
    return spark.table(full_table_name).count()


def get_columns(full_table_name):
    return spark.table(full_table_name).columns


def check_table_exists(layer, table_name):
    full_table_name = f"{CATALOG}.{layer}.{table_name}"

    if table_exists(full_table_name):
        add_result(layer, table_name, "table_exists", "PASS", "Table exists.")
        return True

    add_result(layer, table_name, "table_exists", "FAIL", "Table does not exist.")
    return False


def check_not_empty(layer, table_name):
    full_table_name = f"{CATALOG}.{layer}.{table_name}"

    row_count = get_row_count(full_table_name)

    if row_count > 0:
        add_result(layer, table_name, "row_count", "PASS", f"{row_count:,} rows.")
    else:
        add_result(layer, table_name, "row_count", "FAIL", "Table is empty.")


def check_required_columns(layer, table_name, required_columns):
    full_table_name = f"{CATALOG}.{layer}.{table_name}"
    columns = get_columns(full_table_name)

    missing_columns = [c for c in required_columns if c not in columns]

    if missing_columns:
        add_result(layer, table_name, "required_columns", "FAIL", f"Missing columns: {missing_columns}")
    else:
        add_result(layer, table_name, "required_columns", "PASS", "All required columns exist.")


def check_nulls(layer, table_name, critical_columns):
    full_table_name = f"{CATALOG}.{layer}.{table_name}"
    df = spark.table(full_table_name)

    existing_columns = [c for c in critical_columns if c in df.columns]
    missing_columns = [c for c in critical_columns if c not in df.columns]

    if missing_columns:
        add_result(layer, table_name, "critical_nulls", "FAIL", f"Missing critical columns: {missing_columns}")
        return

    result = (
        df.agg(*[
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in existing_columns
        ])
        .collect()[0]
        .asDict()
    )

    failures = {k: v for k, v in result.items() if v > 0}

    if failures:
        add_result(layer, table_name, "critical_nulls", "FAIL", failures)
    else:
        add_result(layer, table_name, "critical_nulls", "PASS", "No nulls in critical columns.")


def check_duplicate_grain(layer, table_name, grain_columns, severity="FAIL"):
    full_table_name = f"{CATALOG}.{layer}.{table_name}"
    df = spark.table(full_table_name)

    missing_columns = [c for c in grain_columns if c not in df.columns]

    if missing_columns:
        add_result(layer, table_name, "grain_validation", "FAIL", f"Missing grain columns: {missing_columns}")
        return

    duplicate_groups = (
        df.groupBy(*grain_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_groups > 0:
        add_result(
            layer,
            table_name,
            "grain_validation",
            severity,
            f"Duplicate groups found for grain {grain_columns}: {duplicate_groups:,}"
        )
    else:
        add_result(layer, table_name, "grain_validation", "PASS", f"Grain unique: {grain_columns}")


def check_describe_detail(layer, table_name):
    full_table_name = f"{CATALOG}.{layer}.{table_name}"

    try:
        detail = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0].asDict()

        add_result(
            layer,
            table_name,
            "delta_detail",
            "PASS",
            f"format={detail.get('format')}, files={detail.get('numFiles')}, size={detail.get('sizeInBytes')}"
        )
    except Exception as e:
        add_result(layer, table_name, "delta_detail", "FAIL", str(e))


def run_basic_table_checks(layer, table_name, required_columns=None, critical_columns=None, grain_columns=None, grain_severity="FAIL"):
    if not check_table_exists(layer, table_name):
        return

    check_not_empty(layer, table_name)
    check_describe_detail(layer, table_name)

    if required_columns:
        check_required_columns(layer, table_name, required_columns)

    if critical_columns:
        check_nulls(layer, table_name, critical_columns)

    if grain_columns:
        check_duplicate_grain(layer, table_name, grain_columns, grain_severity)


# ========================================
# 2. BRONZE AUDIT
# ========================================

print("=" * 80)
print("BRONZE AUDIT")
print("=" * 80)

bronze_tables = [
    "crm_clients",
    "crm_segmentation",
    "crm_status",
    "erp_orders",
    "erp_order_items",
    "erp_products",
    "erp_salespersons",
    "erp_suppliers",
    "reference_calendar",
    "reference_locations",
    "reference_sales_channels",
    "weather_porto_daily",
    "web_event_logs"
]

for table_name in bronze_tables:
    run_basic_table_checks(
        layer=BRONZE_SCHEMA,
        table_name=table_name,
        required_columns=["ingestion_timestamp", "source_file"],
        critical_columns=None,
        grain_columns=None
    )


# ========================================
# 3. SILVER AUDIT
# ========================================

print("=" * 80)
print("SILVER AUDIT")
print("=" * 80)

silver_configs = {
    "crm_clients": {
        "critical": ["cliente_id", "nome_cliente", "tipo_cliente", "data_registo"],
        "grain": ["cliente_id"],
        "grain_severity": "WARN"
    },
    "crm_segmentation": {
        "critical": ["cliente_id", "segmento", "potencial_valor", "cluster_comercial"],
        "grain": ["cliente_id"],
        "grain_severity": "WARN"
    },
    "crm_status": {
        "critical": ["cliente_id", "status_cliente", "data_status"],
        "grain": ["cliente_id"],
        "grain_severity": "WARN"
    },
    "erp_orders": {
        "critical": ["pedido_id", "data_pedido", "cliente_id", "canal_id", "estado_pedido"],
        "grain": ["pedido_id"],
        "grain_severity": "WARN"
    },
    "erp_order_items": {
        "critical": ["item_pedido_id", "pedido_id", "quantidade", "preco_venda_unitario", "custo_unitario"],
        "grain": ["item_pedido_id"],
        "grain_severity": "FAIL"
    },
    "erp_products": {
        "critical": ["produto_id", "produto_nome", "categoria", "marca", "fornecedor_id", "data_lancamento", "status_produto"],
        "grain": ["produto_id"],
        "grain_severity": "WARN"
    },
    "erp_salespersons": {
        "critical": ["vendedor_id", "nome_vendedor", "data_admissao", "status_vendedor"],
        "grain": ["vendedor_id"],
        "grain_severity": "WARN"
    },
    "erp_suppliers": {
        "critical": ["fornecedor_id", "nome_fornecedor", "pais"],
        "grain": ["fornecedor_id"],
        "grain_severity": "WARN"
    },
    "reference_calendar": {
        "critical": ["data"],
        "grain": ["data"],
        "grain_severity": "WARN"
    },
    "reference_locations": {
        "critical": ["localidade_id", "cidade", "distrito", "regiao"],
        "grain": ["localidade_id"],
        "grain_severity": "FAIL"
    },
    "reference_sales_channels": {
        "critical": ["canal_id", "nome_canal", "descricao"],
        "grain": ["canal_id"],
        "grain_severity": "FAIL"
    },
    "weather_porto_daily": {
        "critical": ["data", "cidade", "fonte_api"],
        "grain": ["data"],
        "grain_severity": "FAIL"
    },
    "web_event_logs": {
        "critical": ["evento_id", "data_hora", "sessao_id", "tipo_evento", "dispositivo", "origem_trafego", "user_agent_family"],
        "grain": ["evento_id"],
        "grain_severity": "FAIL"
    },
    "silver_orders_customers": {
        "critical": ["pedido_id", "cliente_id", "data_pedido"],
        "grain": ["pedido_id"],
        "grain_severity": "FAIL"
    }
}

for table_name, config in silver_configs.items():
    run_basic_table_checks(
        layer=SILVER_SCHEMA,
        table_name=table_name,
        required_columns=None,
        critical_columns=config["critical"],
        grain_columns=config["grain"],
        grain_severity=config["grain_severity"]
    )


# ========================================
# 4. SILVER DOMAIN RULES
# ========================================

print("=" * 80)
print("SILVER DOMAIN RULES")
print("=" * 80)

def check_silver_domain_rules():
    # crm_status: cliente_id duplication is expected/allowed
    df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.crm_status")
    duplicate_client_status = (
        df.groupBy("cliente_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    add_result(SILVER_SCHEMA, "crm_status", "duplicate_cliente_id_monitoring", "PASS", f"{duplicate_client_status:,} duplicate cliente_id groups monitored only.")

    # erp_order_items: produto_id nulls are allowed in Silver and filtered in Gold fact_sales
    df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.erp_order_items")
    null_produto_id = df.filter(F.col("produto_id").isNull()).count()
    add_result(SILVER_SCHEMA, "erp_order_items", "null_produto_id_monitoring", "PASS", f"{null_produto_id:,} rows with null produto_id monitored only.")

    # reference_calendar consistency
    df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.reference_calendar")
    mismatches = (
        df.filter(
            (F.year("data") != F.col("ano")) |
            (F.month("data") != F.col("mes")) |
            (F.dayofmonth("data") != F.col("dia"))
        )
        .count()
    )

    if mismatches > 0:
        add_result(SILVER_SCHEMA, "reference_calendar", "date_decomposition", "FAIL", f"{mismatches:,} mismatches.")
    else:
        add_result(SILVER_SCHEMA, "reference_calendar", "date_decomposition", "PASS", "Date decomposition is consistent.")

    # weather rules
    df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.weather_porto_daily")

    weather_invalid = (
        df.agg(
            F.sum(F.when(F.col("cidade") != "Porto", 1).otherwise(0)).alias("invalid_city"),
            F.sum(F.when(~F.col("choveu").isin([0, 1]), 1).otherwise(0)).alias("invalid_choveu"),
            F.sum(F.when((F.col("humidade_media") < 0) | (F.col("humidade_media") > 100), 1).otherwise(0)).alias("invalid_humidity"),
            F.sum(F.when(F.col("precipitacao_mm") < 0, 1).otherwise(0)).alias("invalid_precipitation"),
            F.sum(F.when(F.col("vento_kmh") < 0, 1).otherwise(0)).alias("invalid_wind"),
            F.sum(
                F.when(
                    (F.col("temperatura_min") > F.col("temperatura_media")) |
                    (F.col("temperatura_media") > F.col("temperatura_max")) |
                    (F.col("temperatura_min") > F.col("temperatura_max")),
                    1
                ).otherwise(0)
            ).alias("invalid_temperature_interval")
        )
        .collect()[0]
        .asDict()
    )

    weather_failures = {k: v for k, v in weather_invalid.items() if v > 0}

    if weather_failures:
        add_result(SILVER_SCHEMA, "weather_porto_daily", "weather_business_rules", "FAIL", weather_failures)
    else:
        add_result(SILVER_SCHEMA, "weather_porto_daily", "weather_business_rules", "PASS", "Weather business rules passed.")

    # web rules
    df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.web_event_logs")

    web_invalid = (
        df.agg(
            F.sum(F.when(~F.col("tipo_evento").isin(["view", "search", "add_to_cart", "purchase"]), 1).otherwise(0)).alias("invalid_tipo_evento"),
            F.sum(F.when(~F.col("dispositivo").isin(["Desktop", "Mobile", "Tablet"]), 1).otherwise(0)).alias("invalid_dispositivo"),
            F.sum(F.when(~F.col("origem_trafego").isin(["Orgânico", "Anúncio", "Direto", "Email", "Social"]), 1).otherwise(0)).alias("invalid_origem_trafego"),
            F.sum(F.when(~F.col("user_agent_family").isin(["Chrome", "Edge", "Firefox", "Safari"]), 1).otherwise(0)).alias("invalid_user_agent_family"),
            F.sum(F.when((F.col("tipo_evento") == "search") & F.col("valor_busca").isNull(), 1).otherwise(0)).alias("invalid_search_logic"),
            F.sum(F.when((F.col("tipo_evento") != "search") & F.col("valor_busca").isNotNull(), 1).otherwise(0)).alias("invalid_non_search_logic"),
            F.sum(F.when(F.col("cliente_id").isNull() & F.col("visitante_id").isNull(), 1).otherwise(0)).alias("invalid_identity_logic"),
            F.sum(F.when(F.col("evento_id") <= 0, 1).otherwise(0)).alias("invalid_evento_id")
        )
        .collect()[0]
        .asDict()
    )

    web_failures = {k: v for k, v in web_invalid.items() if v > 0}

    if web_failures:
        add_result(SILVER_SCHEMA, "web_event_logs", "web_business_rules", "FAIL", web_failures)
    else:
        add_result(SILVER_SCHEMA, "web_event_logs", "web_business_rules", "PASS", "Web business rules passed.")


check_silver_domain_rules()


# ========================================
# 5. GOLD AUDIT
# ========================================

print("=" * 80)
print("GOLD AUDIT")
print("=" * 80)

gold_configs = {
    "dim_calendar": {
        "critical": ["date_key", "data"],
        "grain": ["date_key"],
        "grain_severity": "FAIL"
    },
    "dim_customer": {
        "critical": ["cliente_id"],
        "grain": ["cliente_id"],
        "grain_severity": "FAIL"
    },
    "dim_product": {
        "critical": ["produto_id"],
        "grain": ["produto_id"],
        "grain_severity": "FAIL"
    },
    "dim_sales_channel": {
        "critical": ["canal_id"],
        "grain": ["canal_id"],
        "grain_severity": "FAIL"
    },
    "dim_supplier": {
        "critical": ["fornecedor_id"],
        "grain": ["fornecedor_id"],
        "grain_severity": "FAIL"
    },
    "dim_weather": {
        "critical": ["data"],
        "grain": ["data"],
        "grain_severity": "FAIL"
    },
    "fact_sales": {
        "critical": ["item_pedido_id", "pedido_id", "produto_id", "cliente_id", "canal_id", "data_pedido"],
        "grain": ["item_pedido_id"],
        "grain_severity": "FAIL"
    },
    "mart_sales_performance": {
        "critical": [],
        "grain": None,
        "grain_severity": "FAIL"
    },
    "mart_customer_sales": {
        "critical": [],
        "grain": None,
        "grain_severity": "FAIL"
    },
    "mart_customer_product_mix": {
        "critical": [],
        "grain": None,
        "grain_severity": "FAIL"
    },
    "analytics_sales_overview": {
        "critical": ["data_pedido", "cliente_id", "produto_id", "canal_id"],
        "grain": ["data_pedido", "cliente_id", "produto_id", "canal_id"],
        "grain_severity": "FAIL"
    }
}

for table_name, config in gold_configs.items():
    run_basic_table_checks(
        layer=GOLD_SCHEMA,
        table_name=table_name,
        required_columns=None,
        critical_columns=config["critical"] if config["critical"] else None,
        grain_columns=config["grain"],
        grain_severity=config["grain_severity"]
    )


# ========================================
# 6. GOLD FACT SALES RECONCILIATION
# ========================================

print("=" * 80)
print("GOLD FACT SALES RECONCILIATION")
print("=" * 80)

def check_fact_sales_reconciliation():
    fact = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_sales")
    src = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.erp_order_items").filter(F.col("produto_id").isNotNull())

    src_rows = src.count()
    fact_rows = fact.count()

    if src_rows == fact_rows:
        add_result(GOLD_SCHEMA, "fact_sales", "row_count_reconciliation", "PASS", f"source={src_rows:,}, fact={fact_rows:,}")
    else:
        add_result(GOLD_SCHEMA, "fact_sales", "row_count_reconciliation", "FAIL", f"source={src_rows:,}, fact={fact_rows:,}")

    orphan_result = (
        fact.agg(
            F.sum(F.when(F.col("produto_nome").isNull(), 1).otherwise(0)).alias("orphan_product_rows"),
            F.sum(F.when(F.col("nome_canal").isNull(), 1).otherwise(0)).alias("orphan_channel_rows"),
            F.sum(F.when(F.col("calendar_year").isNull(), 1).otherwise(0)).alias("orphan_calendar_rows"),
            F.sum(F.when(F.col("vendedor_id").isNull(), 1).otherwise(0)).alias("null_vendedor_id")
        )
        .collect()[0]
        .asDict()
    )

    orphan_failures = {k: v for k, v in orphan_result.items() if v > 0}

    if orphan_failures:
        add_result(GOLD_SCHEMA, "fact_sales", "orphan_and_unknown_rules", "FAIL", orphan_failures)
    else:
        add_result(GOLD_SCHEMA, "fact_sales", "orphan_and_unknown_rules", "PASS", "No orphan rows and no null vendedor_id.")

    fact_metrics = (
        fact.agg(
            F.sum("quantity_sold").alias("quantity_sold"),
            F.sum("gross_sales_amount").alias("gross_sales_amount"),
            F.sum("net_sales_amount").alias("net_sales_amount"),
            F.sum("total_cost_amount").alias("total_cost_amount"),
            F.sum("gross_margin_amount").alias("gross_margin_amount"),
            F.sum("line_count").alias("line_count")
        )
        .collect()[0]
        .asDict()
    )

    src_metrics = (
        src.agg(
            F.sum("quantidade").alias("quantity_sold"),
            F.sum(F.col("quantidade") * F.col("preco_lista_unitario")).alias("gross_sales_amount"),
            F.sum(F.col("quantidade") * F.col("preco_venda_unitario")).alias("net_sales_amount"),
            F.sum(F.col("quantidade") * F.col("custo_unitario")).alias("total_cost_amount"),
            F.sum((F.col("quantidade") * F.col("preco_venda_unitario")) - (F.col("quantidade") * F.col("custo_unitario"))).alias("gross_margin_amount"),
            F.count("*").alias("line_count")
        )
        .collect()[0]
        .asDict()
    )

    tolerance = 0.01
    metric_diffs = {}

    for metric in src_metrics:
        src_value = float(src_metrics[metric] or 0)
        fact_value = float(fact_metrics[metric] or 0)
        diff = abs(src_value - fact_value)

        if diff > tolerance:
            metric_diffs[metric] = {
                "source": src_value,
                "fact": fact_value,
                "diff": diff
            }

    if metric_diffs:
        add_result(GOLD_SCHEMA, "fact_sales", "metric_reconciliation", "FAIL", metric_diffs)
    else:
        add_result(GOLD_SCHEMA, "fact_sales", "metric_reconciliation", "PASS", "Financial and quantity metrics reconciled.")

    aov_mismatches = (
        fact
        .withColumn("expected_aov", F.sum("net_sales_amount").over(Window.partitionBy("pedido_id")))
        .filter(F.abs(F.col("average_order_value") - F.col("expected_aov")) > tolerance)
        .count()
    )

    if aov_mismatches > 0:
        add_result(GOLD_SCHEMA, "fact_sales", "average_order_value", "FAIL", f"{aov_mismatches:,} mismatched rows.")
    else:
        add_result(GOLD_SCHEMA, "fact_sales", "average_order_value", "PASS", "average_order_value matches net sales by pedido_id.")


from pyspark.sql.window import Window
check_fact_sales_reconciliation()


# ========================================
# 7. ANALYTICS RECONCILIATION
# ========================================

print("=" * 80)
print("ANALYTICS RECONCILIATION")
print("=" * 80)

def check_analytics_reconciliation():
    fact = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_sales")
    analytics = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.analytics_sales_overview")

    fact_metrics = (
        fact.agg(
            F.sum("quantity_sold").alias("quantity_sold"),
            F.sum("gross_sales_amount").alias("gross_sales_amount"),
            F.sum("net_sales_amount").alias("net_sales_amount"),
            F.sum("total_cost_amount").alias("total_cost_amount"),
            F.sum("gross_margin_amount").alias("gross_margin_amount"),
            F.sum("line_count").alias("line_count")
        )
        .collect()[0]
        .asDict()
    )

    analytics_metrics = (
        analytics.agg(
            F.sum("quantity_sold").alias("quantity_sold"),
            F.sum("gross_sales_amount").alias("gross_sales_amount"),
            F.sum("net_sales_amount").alias("net_sales_amount"),
            F.sum("total_cost_amount").alias("total_cost_amount"),
            F.sum("gross_margin_amount").alias("gross_margin_amount"),
            F.sum("line_count").alias("line_count")
        )
        .collect()[0]
        .asDict()
    )

    tolerance = 0.01
    metric_diffs = {}

    for metric in fact_metrics:
        fact_value = float(fact_metrics[metric] or 0)
        analytics_value = float(analytics_metrics[metric] or 0)
        diff = abs(fact_value - analytics_value)

        if diff > tolerance:
            metric_diffs[metric] = {
                "fact": fact_value,
                "analytics": analytics_value,
                "diff": diff
            }

    if metric_diffs:
        add_result(GOLD_SCHEMA, "analytics_sales_overview", "analytics_vs_fact_reconciliation", "FAIL", metric_diffs)
    else:
        add_result(GOLD_SCHEMA, "analytics_sales_overview", "analytics_vs_fact_reconciliation", "PASS", "Analytics metrics reconcile with fact_sales.")


check_analytics_reconciliation()


# ========================================
# 8. FINAL SUMMARY
# ========================================

print("=" * 80)
print("AUDIT SUMMARY")
print("=" * 80)

summary_df = spark.createDataFrame(audit_results)

summary = (
    summary_df
    .groupBy("status")
    .count()
    .collect()
)

summary_dict = {row["status"]: row["count"] for row in summary}

pass_count = summary_dict.get("PASS", 0)
warn_count = summary_dict.get("WARN", 0)
fail_count = summary_dict.get("FAIL", 0)

print(f"PASS: {pass_count}")
print(f"WARN: {warn_count}")
print(f"FAIL: {fail_count}")

print("=" * 80)
print("FAILED CHECKS")
print("=" * 80)

failed = [r for r in audit_results if r["status"] == "FAIL"]

if failed:
    for r in failed:
        print(f"[FAIL] {r['layer']}.{r['table']} | {r['check_name']} | {r['details']}")
else:
    print("[INFO] No failed checks detected.")

print("=" * 80)
print("WARNING CHECKS")
print("=" * 80)

warnings = [r for r in audit_results if r["status"] == "WARN"]

if warnings:
    for r in warnings:
        print(f"[WARN] {r['layer']}.{r['table']} | {r['check_name']} | {r['details']}")
else:
    print("[INFO] No warning checks detected.")

if FAIL_ON_ERRORS and fail_count > 0:
    raise ValueError(f"Data quality audit failed with {fail_count} failed checks.")

print("=" * 80)
print("DATA QUALITY AUDIT COMPLETED SUCCESSFULLY")
print("=" * 80)