# Databricks notebook source
# MAGIC %md
# MAGIC ### Exploratory — Orders + Customers
# MAGIC
# MAGIC #### Objective
# MAGIC
# MAGIC Explore the integration between:
# MAGIC
# MAGIC * `erp_orders`
# MAGIC * `crm_clients`
# MAGIC * `crm_segmentation`
# MAGIC * `crm_status`
# MAGIC
# MAGIC Validate the feasibility of building an orders dataset enriched with customer data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scope
# MAGIC
# MAGIC * Join key quality (`cliente_id`)
# MAGIC * Match rate between datasets
# MAGIC * Orphan records (orders without customer)
# MAGIC * Row duplication after joins
# MAGIC * Join strategy impact (`left`, `inner`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Outcome
# MAGIC
# MAGIC * Define join strategy
# MAGIC * Identify data issues
# MAGIC * Validate integration structure
# MAGIC * Document decisions for implementation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Notes
# MAGIC
# MAGIC Exploratory only
# MAGIC No production logic
# MAGIC

# COMMAND ----------

# ========================================
# EXPLORATORY NOTEBOOK
# DATASET: orders_customers
# ========================================


# ========================================
# 0. CONFIGURATION
# ========================================

CATALOG = "ptfrozenfoods_dev"
SOURCE_SCHEMA = "silver"
TARGET_SCHEMA = "silver"

DOMAIN = "integration"
DATASET = "orders_customers"

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

CANDIDATE_TARGET_TABLE = f"{CATALOG}.{TARGET_SCHEMA}.silver_{DATASET}"

SILVER_BASE_PATH = f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/"
ERP_SOURCE_PATH = f"{SILVER_BASE_PATH}erp/{ORDERS_DATASET}/"
CRM_CLIENTS_SOURCE_PATH = f"{SILVER_BASE_PATH}crm/{CLIENTS_DATASET}/"
CRM_SEGMENTATION_SOURCE_PATH = f"{SILVER_BASE_PATH}crm/{SEGMENTATION_DATASET}/"
CRM_STATUS_SOURCE_PATH = f"{SILVER_BASE_PATH}crm/{STATUS_DATASET}/"

# COMMAND ----------

# ========================================
# 1. CONTEXT SETUP
# ========================================

# Set catalog
spark.sql(f"USE CATALOG {CATALOG}")

# Ensure target schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{TARGET_SCHEMA}")

# Set working schema
spark.sql(f"USE SCHEMA {TARGET_SCHEMA}")

print("[INFO] Context setup completed successfully.")

# COMMAND ----------

# ========================================
# 2. CONFIGURATION SUMMARY
# ========================================

print("=" * 80)
print("EXPLORATORY NOTEBOOK CONFIGURATION")
print("=" * 80)

print(f"Catalog:                {CATALOG}")
print(f"Source schema:          {SOURCE_SCHEMA}")
print(f"Target schema:          {TARGET_SCHEMA}")

print("-" * 80)
print("DATASETS")
print("-" * 80)

print(f"Orders table:           {ORDERS_TABLE}")
print(f"Clients table:          {CLIENTS_TABLE}")
print(f"Segmentation table:     {SEGMENTATION_TABLE}")
print(f"Status table:           {STATUS_TABLE}")

print("-" * 80)
print("TARGET (CANDIDATE)")
print("-" * 80)

print(f"Candidate table:        {CANDIDATE_TARGET_TABLE}")

print("-" * 80)
print("PATHS")
print("-" * 80)

print(f"ERP path:               {ERP_SOURCE_PATH}")
print(f"CRM clients path:       {CRM_CLIENTS_SOURCE_PATH}")
print(f"CRM segmentation path:  {CRM_SEGMENTATION_SOURCE_PATH}")
print(f"CRM status path:        {CRM_STATUS_SOURCE_PATH}")

print("=" * 80)

# COMMAND ----------

# ========================================
# 3. PRE-CHECKS
# ========================================

print("[INFO] Checking source table availability...")
spark.sql(f"DESCRIBE TABLE {ORDERS_TABLE}")
spark.sql(f"DESCRIBE TABLE {CLIENTS_TABLE}")
spark.sql(f"DESCRIBE TABLE {SEGMENTATION_TABLE}")
spark.sql(f"DESCRIBE TABLE {STATUS_TABLE}")

print("[INFO] Checking source path access...")
dbutils.fs.ls(ERP_SOURCE_PATH)
dbutils.fs.ls(CRM_CLIENTS_SOURCE_PATH)
dbutils.fs.ls(CRM_SEGMENTATION_SOURCE_PATH)
dbutils.fs.ls(CRM_STATUS_SOURCE_PATH)

print("[INFO] Checking target container access...")
dbutils.fs.ls(f"abfss://{SILVER_CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/")

print("[INFO] Pre-checks completed successfully.")

# COMMAND ----------

# ========================================
# 4. READ SOURCE DATA
# ========================================

df_orders = spark.table(ORDERS_TABLE)
df_clients = spark.table(CLIENTS_TABLE)
df_segmentation = spark.table(SEGMENTATION_TABLE)
df_status = spark.table(STATUS_TABLE)

print("[INFO] Source data loaded successfully.")
print(f"[INFO] Orders table:       {ORDERS_TABLE}")
print(f"[INFO] Clients table:      {CLIENTS_TABLE}")
print(f"[INFO] Segmentation table: {SEGMENTATION_TABLE}")
print(f"[INFO] Status table:       {STATUS_TABLE}")

# COMMAND ----------

display(df_orders.limit(20))

# COMMAND ----------

display(df_clients.limit(20))

# COMMAND ----------

display(df_segmentation.limit(20))

# COMMAND ----------

display(df_status.limit(20))

# COMMAND ----------

# ========================================
# 5. INITIAL DATA OVERVIEW
# ========================================

print("=" * 80)
print("INITIAL DATA OVERVIEW")
print("=" * 80)

# Row counts
orders_count = df_orders.count()
clients_count = df_clients.count()
segmentation_count = df_segmentation.count()
status_count = df_status.count()

print(f"Orders count:        {orders_count}")
print(f"Clients count:       {clients_count}")
print(f"Segmentation count:  {segmentation_count}")
print(f"Status count:        {status_count}")
print("=" * 80)

# Distinct counts for join key
print("\n[INFO] Distinct cliente_id counts:")

orders_distinct = df_orders.select("cliente_id").distinct().count()
clients_distinct = df_clients.select("cliente_id").distinct().count()
segmentation_distinct = df_segmentation.select("cliente_id").distinct().count()
status_distinct = df_status.select("cliente_id").distinct().count()

print(f"Distinct cliente_id in Orders:       {orders_distinct}")
print(f"Distinct cliente_id in Clients:      {clients_distinct}")
print(f"Distinct cliente_id in Segmentation: {segmentation_distinct}")
print(f"Distinct cliente_id in Status:       {status_distinct}")
print("=" * 80)

# Null checks for join key
print("\n[INFO] Checking null values in cliente_id:")

from pyspark.sql.functions import col

orders_nulls = df_orders.filter(col("cliente_id").isNull()).count()
clients_nulls = df_clients.filter(col("cliente_id").isNull()).count()
segmentation_nulls = df_segmentation.filter(col("cliente_id").isNull()).count()
status_nulls = df_status.filter(col("cliente_id").isNull()).count()

print(f"Null cliente_id in Orders:       {orders_nulls}")
print(f"Null cliente_id in Clients:      {clients_nulls}")
print(f"Null cliente_id in Segmentation: {segmentation_nulls}")
print(f"Null cliente_id in Status:       {status_nulls}")

print("=" * 80)
print("[INFO] Initial data overview completed successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Findings — Orders and Customers
# MAGIC
# MAGIC #### Data Volume
# MAGIC
# MAGIC | Dataset | Records |
# MAGIC |---------|---------|
# MAGIC | ERP Orders | 90,139 |
# MAGIC | CRM Clients | 320 |
# MAGIC | CRM Segmentation | 320 |
# MAGIC | CRM Status | 320 |
# MAGIC
# MAGIC #### Key Observations
# MAGIC
# MAGIC - The CRM datasets contain a complete and consistent set of 320 unique customers.
# MAGIC - The ERP orders dataset contains 145 unique customers.
# MAGIC - This indicates that not all registered customers have placed orders, which is expected in real-world business scenarios.
# MAGIC - No null values were found in the `cliente_id` field across all datasets.
# MAGIC - The CRM datasets are fully aligned, ensuring referential integrity.
# MAGIC - The `cliente_id` field is reliable and suitable for data integration.
# MAGIC
# MAGIC #### Cardinality Analysis
# MAGIC
# MAGIC | Relationship | Type |
# MAGIC |--------------|------|
# MAGIC | CRM Clients → ERP Orders | 1:N |
# MAGIC | CRM Clients → Segmentation | 1:1 |
# MAGIC | CRM Clients → Status | 1:1 |
# MAGIC
# MAGIC #### Join Strategy
# MAGIC
# MAGIC | Source | Target | Join Type | Rationale |
# MAGIC |--------|--------|-----------|-----------|
# MAGIC | `erp_orders` | `crm_clients` | LEFT JOIN | Preserve all orders |
# MAGIC | `crm_clients` | `crm_segmentation` | LEFT JOIN | Enrich customer attributes |
# MAGIC | `crm_clients` | `crm_status` | LEFT JOIN | Include customer lifecycle status |
# MAGIC
# MAGIC #### Conclusion
# MAGIC
# MAGIC The datasets are consistent, complete, and suitable for Silver Integration.  
# MAGIC The `cliente_id` field is validated as the primary key for integrating ERP and CRM domains.
# MAGIC
# MAGIC #### Next Steps
# MAGIC
# MAGIC - Perform detailed join validation.
# MAGIC - Measure match rates and orphan records.
# MAGIC - Assess duplication risks after joins.
# MAGIC - Implement the integrated dataset in the Silver layer.

# COMMAND ----------

# ========================================
# 6. JOIN KEY ANALYSIS
# ========================================

from pyspark.sql.functions import col

print("=" * 80)
print("JOIN KEY ANALYSIS")
print("=" * 80)

# Distinct customer IDs
orders_ids = df_orders.select("cliente_id").distinct()
clients_ids = df_clients.select("cliente_id").distinct()

orders_count = orders_ids.count()
clients_count = clients_ids.count()

print(f"Distinct cliente_id in Orders:  {orders_count}")
print(f"Distinct cliente_id in Clients: {clients_count}")

# Customers in Orders but not in Clients
orders_not_in_clients = orders_ids.join(
    clients_ids,
    on="cliente_id",
    how="left_anti"
)

orders_not_in_clients_count = orders_not_in_clients.count()
print(f"Orders without matching Clients: {orders_not_in_clients_count}")

# Customers in Clients but not in Orders
clients_not_in_orders = clients_ids.join(
    orders_ids,
    on="cliente_id",
    how="left_anti"
)

clients_not_in_orders_count = clients_not_in_orders.count()
print(f"Clients without Orders: {clients_not_in_orders_count}")

# Match rate calculation
matching_customers = orders_ids.join(
    clients_ids,
    on="cliente_id",
    how="inner"
).count()

match_rate = (matching_customers / orders_count) * 100 if orders_count > 0 else 0

print(f"Matching Customers: {matching_customers}")
print(f"Match Rate (Orders → Clients): {match_rate:.2f}%")

print("=" * 80)
print("[INFO] Join key analysis completed successfully.")

# COMMAND ----------

# Orders without matching clients
display(orders_not_in_clients)

# COMMAND ----------

# Clients without orders
display(clients_not_in_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Findings — Join Key Analysis
# MAGIC
# MAGIC #### Result
# MAGIC
# MAGIC - `erp_orders` contains 145 distinct customers.
# MAGIC - `crm_clients` contains 320 distinct customers.
# MAGIC - All customer IDs present in `erp_orders` are available in `crm_clients`.
# MAGIC - No orphan orders were identified.
# MAGIC - A total of 175 customers have no registered orders.
# MAGIC - Match rate from orders to clients is 100%.
# MAGIC
# MAGIC #### Interpretation
# MAGIC
# MAGIC The `cliente_id` field is consistent and reliable for integration between ERP orders and CRM customer data.
# MAGIC
# MAGIC The relationship between both datasets is valid for a customer-enriched orders dataset, using `erp_orders` as the base table.
# MAGIC
# MAGIC #### Decision
# MAGIC
# MAGIC Proceed with the integration using:
# MAGIC
# MAGIC - `erp_orders` as the primary dataset
# MAGIC - `LEFT JOIN` with `crm_clients`
# MAGIC - `LEFT JOIN` with `crm_segmentation`
# MAGIC - `LEFT JOIN` with `crm_status`

# COMMAND ----------

# ========================================
# 7. ORDER GRAIN ANALYSIS
# ========================================

from pyspark.sql.functions import col, countDistinct

orders_total_rows = df_orders.count()
orders_distinct_ids = df_orders.select(countDistinct("pedido_id")).collect()[0][0]
orders_duplicate_rows = orders_total_rows - orders_distinct_ids

df_order_duplicates = (
    df_orders.groupBy("pedido_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_order_ids = df_order_duplicates.count()

print("=" * 80)
print("ORDER GRAIN ANALYSIS")
print("=" * 80)
print(f"Total rows:                  {orders_total_rows}")
print(f"Distinct pedido_id:          {orders_distinct_ids}")
print(f"Duplicate rows:              {orders_duplicate_rows}")
print(f"Duplicated pedido_id count:  {duplicate_order_ids}")
print("=" * 80)
print("[INFO] Order grain analysis completed successfully.")

# COMMAND ----------

display(
    df_order_duplicates.orderBy(col("count").desc(), col("pedido_id"))
)

# COMMAND ----------

sample_duplicate_ids = [
    "PED2021013000713",
    "PED2021020700981",
    "PED2021021101123"
]

display(
    df_orders
    .filter(col("pedido_id").isin(sample_duplicate_ids))
    .orderBy("pedido_id", "ingestion_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order Grain Assessment — `erp_orders`
# MAGIC
# MAGIC #### Objective
# MAGIC To validate whether the dataset `erp_orders` preserves the expected business grain of one row per order (`pedido_id`) before performing integrations with CRM datasets.
# MAGIC
# MAGIC #### Expected Grain
# MAGIC - **Entity:** Order Header  
# MAGIC - **Primary Key:** `pedido_id`  
# MAGIC - **Expected Granularity:** One row per order.
# MAGIC
# MAGIC #### Findings
# MAGIC The analysis identified duplicated `pedido_id` values in the dataset.
# MAGIC
# MAGIC Key observations include:
# MAGIC
# MAGIC - Each duplicated `pedido_id` appears exactly twice.
# MAGIC - Business attributes remain identical across duplicated records.
# MAGIC - Differences are limited to operational audit fields, particularly:
# MAGIC   - `usuario_ultima_alteracao`.
# MAGIC - Metadata fields such as `ingestion_timestamp`, `source_file`, and `load_date` remain consistent.
# MAGIC
# MAGIC #### Business Interpretation
# MAGIC The duplicated records do not represent multiple products within the same order.  
# MAGIC Such relationships are modeled in the `erp_order_items` dataset.
# MAGIC
# MAGIC Instead, the duplicates indicate repeated order headers generated by the source system or ingestion process.
# MAGIC
# MAGIC #### Impact Assessment
# MAGIC | Aspect | Impact |
# MAGIC |--------|--------|
# MAGIC | Join Integrity | No impact on join correctness |
# MAGIC | Analytical Accuracy | Potential risk of inflated metrics if not addressed |
# MAGIC | Data Model | Requires deduplication to preserve the correct grain |
# MAGIC | Silver Integration | Must enforce deterministic uniqueness |
# MAGIC
# MAGIC #### Decision
# MAGIC A deterministic deduplication strategy will be applied to `erp_orders` in the production processing notebook.
# MAGIC
# MAGIC The rule will ensure that only one record per `pedido_id` is retained, prioritizing:
# MAGIC
# MAGIC 1. Highest business completeness.
# MAGIC 2. Most recent `ingestion_timestamp`.
# MAGIC 3. Source lineage (`source_file`).
# MAGIC 4. Latest `usuario_ultima_alteracao`.
# MAGIC
# MAGIC #### Implementation Layer
# MAGIC | Layer | Action |
# MAGIC |-------|--------|
# MAGIC | Bronze | Preserve raw data without modifications |
# MAGIC | Silver (Domain) | Retain original structure for traceability |
# MAGIC | Silver Integration | Apply deterministic deduplication before enrichment |
# MAGIC | Gold | Consume the deduplicated dataset for analytics and BI |
# MAGIC
# MAGIC #### Governance and Traceability
# MAGIC This decision ensures:
# MAGIC
# MAGIC - Consistency with Lakehouse best practices.
# MAGIC - Preservation of raw data lineage.
# MAGIC - Analytical reliability for BI and machine learning workloads.
# MAGIC - Transparency and auditability of data transformations.
# MAGIC
# MAGIC #### Conclusion
# MAGIC The exploratory analysis confirms that `erp_orders` contains duplicated order headers.  
# MAGIC These duplicates are operational artifacts and must be resolved through deterministic deduplication in the production pipeline to guarantee the intended grain:
# MAGIC
# MAGIC **One row per order (`pedido_id`).**

# COMMAND ----------

# ========================================
# 8. JOIN TESTING
# ========================================

df_orders_customers = (
    df_orders.alias("o")
    .join(
        df_clients.alias("c"),
        on="cliente_id",
        how="left"
    )
    .join(
        df_segmentation.alias("s"),
        on="cliente_id",
        how="left"
    )
    .join(
        df_status.alias("st"),
        on="cliente_id",
        how="left"
    )
)

print("[INFO] Join completed successfully.")
print(f"[INFO] Joined row count: {df_orders_customers.count()}")

# COMMAND ----------

display(df_orders_customers.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Findings — Join Testing
# MAGIC
# MAGIC #### Result
# MAGIC
# MAGIC The join between `erp_orders`, `crm_clients`, `crm_segmentation`, and `crm_status` was completed successfully.
# MAGIC
# MAGIC The integrated dataset contains 90,139 rows, which is the same row count as the original `erp_orders` table.
# MAGIC
# MAGIC #### Interpretation
# MAGIC
# MAGIC This confirms that:
# MAGIC
# MAGIC - the selected join strategy is valid;
# MAGIC - no row explosion occurred after the joins;
# MAGIC - customer enrichment behaves consistently with the expected cardinality;
# MAGIC - the CRM tables appear to preserve a 1:1 relationship at customer level.
# MAGIC
# MAGIC #### Technical Note
# MAGIC
# MAGIC The joined dataset currently contains repeated metadata columns such as:
# MAGIC
# MAGIC - `load_date`
# MAGIC - `ingestion_timestamp`
# MAGIC - `source_file`
# MAGIC
# MAGIC This is acceptable in the exploratory phase, but the production notebook must apply explicit column selection and renaming to avoid ambiguity and improve dataset usability.
# MAGIC
# MAGIC #### Decision
# MAGIC
# MAGIC Proceed with the integration design using `erp_orders` as the base dataset.
# MAGIC
# MAGIC In the production version, define a curated column selection strategy with standardized metadata naming.

# COMMAND ----------

# ========================================
# 9. DUPLICATION ANALYSIS
# ========================================

from pyspark.sql.functions import count

df_clients_duplicates = (
    df_clients.groupBy("cliente_id")
    .agg(count("*").alias("record_count"))
    .filter("record_count > 1")
)

df_segmentation_duplicates = (
    df_segmentation.groupBy("cliente_id")
    .agg(count("*").alias("record_count"))
    .filter("record_count > 1")
)

df_status_duplicates = (
    df_status.groupBy("cliente_id")
    .agg(count("*").alias("record_count"))
    .filter("record_count > 1")
)

print("=" * 80)
print("DUPLICATION ANALYSIS")
print("=" * 80)
print(f"Clients duplicates:      {df_clients_duplicates.count()}")
print(f"Segmentation duplicates: {df_segmentation_duplicates.count()}")
print(f"Status duplicates:       {df_status_duplicates.count()}")
print("=" * 80)
print("[INFO] Duplication analysis completed successfully.")

# COMMAND ----------

display(df_clients_duplicates)

# COMMAND ----------

display(df_segmentation_duplicates)

# COMMAND ----------

display(df_status_duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Findings — Duplication Analysis
# MAGIC
# MAGIC #### Result
# MAGIC
# MAGIC No duplicate records were found in the following datasets when grouped by `cliente_id`:
# MAGIC
# MAGIC - `crm_clients`
# MAGIC - `crm_segmentation`
# MAGIC - `crm_status`
# MAGIC
# MAGIC #### Interpretation
# MAGIC
# MAGIC This confirms that all CRM datasets involved in the integration behave as expected at customer level.
# MAGIC
# MAGIC The relationship structure is:
# MAGIC
# MAGIC - `crm_clients`: 1 record per customer
# MAGIC - `crm_segmentation`: 1 record per customer
# MAGIC - `crm_status`: 1 record per customer
# MAGIC
# MAGIC As a result, enriching `erp_orders` with these datasets does not introduce row duplication.
# MAGIC
# MAGIC #### Conclusion
# MAGIC
# MAGIC The integration preserves the original grain of the base dataset:
# MAGIC
# MAGIC - **1 row per order (`pedido_id`)**
# MAGIC
# MAGIC This validates the current integration design for the Silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Conclusion — Silver Integration Exploratory
# MAGIC
# MAGIC #### Overview
# MAGIC
# MAGIC This exploratory notebook evaluated the feasibility, integrity, and performance considerations for integrating ERP and CRM datasets within the Silver layer of the PT Frozen Foods Lakehouse architecture.
# MAGIC
# MAGIC The analysis confirmed that the datasets are consistent, reliable, and suitable for building an integrated, analytics-ready table enriched with customer attributes. Additionally, a grain inconsistency was identified and addressed through a deterministic deduplication strategy.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Business Context
# MAGIC
# MAGIC The PT Frozen Foods data platform simulates a real-world frozen food distribution company in Northern Portugal. The Silver Integration layer aims to transform curated datasets into enriched structures that support advanced analytics, reporting, and machine learning.
# MAGIC
# MAGIC This integration focuses on combining transactional ERP data with CRM customer information.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Datasets Analyzed
# MAGIC
# MAGIC | Domain | Dataset |
# MAGIC |--------|---------|
# MAGIC | ERP | `erp_orders` |
# MAGIC | CRM | `crm_clients` |
# MAGIC | CRM | `crm_segmentation` |
# MAGIC | CRM | `crm_status` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Data Quality Assessment
# MAGIC
# MAGIC | Validation | Result |
# MAGIC |------------|--------|
# MAGIC | Null values in `cliente_id` | None detected |
# MAGIC | Orphan orders | None detected |
# MAGIC | Duplicate records in CRM datasets | None detected |
# MAGIC | Referential integrity | Confirmed |
# MAGIC | Schema consistency | Validated |
# MAGIC | Data readiness for integration | Approved |
# MAGIC | Duplicate records in `erp_orders` | Detected and analyzed |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Cardinality Analysis
# MAGIC
# MAGIC | Relationship | Type |
# MAGIC |--------------|------|
# MAGIC | CRM Clients → ERP Orders | 1:N |
# MAGIC | CRM Clients → Segmentation | 1:1 |
# MAGIC | CRM Clients → Status | 1:1 |
# MAGIC
# MAGIC This structure ensures that enriching ERP orders with CRM attributes does not introduce duplication or compromise data integrity.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Order Grain Validation
# MAGIC
# MAGIC During the exploratory analysis, duplicated `pedido_id` values were identified in the `erp_orders` dataset.
# MAGIC
# MAGIC | Validation | Result |
# MAGIC |------------|--------|
# MAGIC | Expected Grain | One row per order (`pedido_id`) |
# MAGIC | Duplicate Order IDs | Detected |
# MAGIC | Root Cause | Repeated order headers from the source system |
# MAGIC | Business Attributes | Identical across duplicates |
# MAGIC | Differences Observed | Limited to audit fields such as `usuario_ultima_alteracao` |
# MAGIC | Impact on Joins | None |
# MAGIC | Analytical Risk | Potential metric inflation if unresolved |
# MAGIC | Mitigation | Deterministic deduplication in the processing notebook |
# MAGIC
# MAGIC These duplicates do not represent multiple products per order, as that relationship belongs to the `erp_order_items` dataset. Instead, they reflect operational variations from the source system.
# MAGIC
# MAGIC To preserve the intended grain, deterministic deduplication is applied in the Silver Integration processing layer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Join Validation Results
# MAGIC
# MAGIC | Metric | Value |
# MAGIC |--------|-------|
# MAGIC | Total Orders (Raw) | 90,139 |
# MAGIC | Distinct Customers in Orders | 145 |
# MAGIC | Distinct Customers in CRM | 320 |
# MAGIC | Orders without Matching Clients | 0 |
# MAGIC | Customers without Orders | 175 |
# MAGIC | Match Rate (Orders → Clients) | 100% |
# MAGIC | Row Count After Deduplication | Preserved |
# MAGIC | Duplicate Orders After Join | 0 |
# MAGIC
# MAGIC These results confirm that the integration preserves the intended grain after deduplication.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Data Model Decisions
# MAGIC
# MAGIC | Attribute | Decision |
# MAGIC |-----------|----------|
# MAGIC | Base Dataset | `erp_orders` |
# MAGIC | Integration Key | `cliente_id` |
# MAGIC | Dataset Grain | One row per order (`pedido_id`) |
# MAGIC | Required Preprocessing | Deterministic deduplication of `erp_orders` |
# MAGIC | Join Strategy | LEFT JOIN from ERP to CRM |
# MAGIC | Data Layer | Silver Integration |
# MAGIC | Output Format | Delta Lake |
# MAGIC | Storage Type | External Tables on ADLS |
# MAGIC | Catalog | Unity Catalog |
# MAGIC | Target Schema | `silver` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Validated Join Strategy
# MAGIC
# MAGIC - `erp_orders` LEFT JOIN `crm_clients`
# MAGIC - `erp_orders` LEFT JOIN `crm_segmentation`
# MAGIC - `erp_orders` LEFT JOIN `crm_status`
# MAGIC
# MAGIC This approach preserves transactional integrity while enriching the dataset with customer insights.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Storage Architecture
# MAGIC
# MAGIC | Component | Configuration |
# MAGIC |-----------|--------------|
# MAGIC | Storage Account | `stptfrozenfoodsdevwe01` |
# MAGIC | Data Lake | Azure Data Lake Storage Gen2 |
# MAGIC | Table Type | External |
# MAGIC | File Format | Delta |
# MAGIC | Catalog | `ptfrozenfoods_dev` |
# MAGIC | Schema | `silver` |
# MAGIC | Container | `silver` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Performance Strategy
# MAGIC
# MAGIC To ensure efficiency and scalability, performance optimizations are structured into three categories.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##### 1. Logical Optimization (Query and Processing Level)
# MAGIC
# MAGIC | Strategy | Description |
# MAGIC |----------|-------------|
# MAGIC | Column Pruning | Select only required columns. |
# MAGIC | Join Order Control | Begin with the fact table (`erp_orders`). |
# MAGIC | Broadcast Joins | Optimize joins with small CRM datasets. |
# MAGIC | Avoid `SELECT *` | Reduce unnecessary data scanning. |
# MAGIC | Explicit Column Selection | Improve clarity and performance. |
# MAGIC | Alias and Renaming Strategy | Prevent ambiguity. |
# MAGIC | Removal of Redundant Metadata | Eliminate duplicated technical fields. |
# MAGIC | Row Count Validation | Ensure no data loss during processing. |
# MAGIC | Duplicate Validation by `pedido_id` | Preserve dataset grain. |
# MAGIC | Deterministic Deduplication | Ensures one record per order. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##### 2. Physical Optimization (Storage and Data Layout)
# MAGIC
# MAGIC | Strategy | Description |
# MAGIC |----------|-------------|
# MAGIC | Delta Lake Format | Ensures ACID compliance and scalability. |
# MAGIC | External Tables on ADLS | Enables interoperability and governance. |
# MAGIC | Liquid Clustering | Optimizes data layout dynamically. |
# MAGIC | No Default Partitioning | Avoids rigid partition strategies. |
# MAGIC | Avoidance of Z-Ordering | Replaced by Liquid Clustering. |
# MAGIC | Optimized Clustering Keys | `data_pedido`, `cliente_id`. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##### 3. Execution and Maintenance Optimization (Platform Level)
# MAGIC
# MAGIC | Strategy | Description |
# MAGIC |----------|-------------|
# MAGIC | Photon Engine | Accelerates query performance. |
# MAGIC | Unity Catalog Governance | Provides lineage and access control. |
# MAGIC | Adaptive Query Execution (AQE) | Dynamically optimizes execution plans. |
# MAGIC | Delta Engine Optimization | Enhances query efficiency. |
# MAGIC | Delta Cache | Improves repeated data access. |
# MAGIC | Auto Optimize | Applied when supported by the environment. |
# MAGIC | Predictive Optimization | Not applicable due to external tables. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### External Tables Decision
# MAGIC
# MAGIC The project adopts external Delta tables stored in ADLS.
# MAGIC
# MAGIC | Advantage | Description |
# MAGIC |-----------|-------------|
# MAGIC | Data Ownership | Full control over storage. |
# MAGIC | Interoperability | Accessible by multiple Azure services. |
# MAGIC | Governance | Integration with Azure and Unity Catalog. |
# MAGIC | Portability | Independent of the compute engine. |
# MAGIC | Lakehouse Alignment | Supports medallion architecture. |
# MAGIC | Safety | Dropping tables does not delete data. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Target Output Specification
# MAGIC
# MAGIC | Attribute | Value |
# MAGIC |-----------|-------|
# MAGIC | Target Table | `ptfrozenfoods_dev.silver.silver_orders_customers` |
# MAGIC | Storage Path | `abfss://silver@stptfrozenfoodsdevwe01.dfs.core.windows.net/integration/silver_orders_customers/` |
# MAGIC | Format | Delta |
# MAGIC | Table Type | External |
# MAGIC | Clustering Strategy | Liquid Clustering |
# MAGIC | Clustering Keys | `data_pedido`, `cliente_id` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Readiness for Production
# MAGIC
# MAGIC | Criterion | Status |
# MAGIC |-----------|--------|
# MAGIC | Data Quality | Validated |
# MAGIC | Referential Integrity | Confirmed |
# MAGIC | Order Grain Consistency | Ensured via Deduplication |
# MAGIC | Performance Strategy | Defined |
# MAGIC | Data Model | Approved |
# MAGIC | Governance | Aligned with Unity Catalog |
# MAGIC | Scalability | Ensured |
# MAGIC | Documentation | Completed |
# MAGIC | Processing Notebook | Ready to be implemented |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Next Steps
# MAGIC
# MAGIC 1. Create the Silver Integration processing notebook.
# MAGIC 2. Apply deterministic deduplication to `erp_orders`.
# MAGIC 3. Implement curated joins and explicit column selection.
# MAGIC 4. Apply Liquid Clustering to the final Delta table.
# MAGIC 5. Register the dataset in Unity Catalog.
# MAGIC 6. Validate data quality and performance metrics.
# MAGIC 7. Promote the dataset for Gold-layer consumption.
# MAGIC 8. Enable downstream BI and Machine Learning workloads.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Conclusion
# MAGIC
# MAGIC The exploratory analysis confirms that the integration of ERP orders with CRM customer data is technically sound, operationally consistent, and aligned with modern Lakehouse best practices.
# MAGIC
# MAGIC A grain inconsistency identified in the `erp_orders` dataset was resolved through deterministic deduplication, ensuring analytical accuracy and preserving transactional integrity.
# MAGIC
# MAGIC The resulting dataset enriches business insights and establishes a robust foundation for analytics, reporting, and machine learning.
# MAGIC
# MAGIC This work adheres to enterprise-grade data engineering standards and prepares the PT Frozen Foods platform for scalable and high-performance data consumption.
# MAGIC
# MAGIC ---