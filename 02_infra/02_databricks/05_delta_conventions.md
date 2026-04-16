# Delta Lake Conventions

## Overview

> **Note:** This document outlines the standard Delta Lake conventions adopted in the PT Frozen Foods project. It provides general guidelines aligned with industry best practices and may be adapted as the platform evolves.

This document defines the Delta Lake standards adopted in the **PT Frozen Foods** project. These conventions ensure data reliability, performance, scalability, and governance across the Lakehouse architecture implemented on Azure Databricks.

Delta Lake serves as the foundational storage layer for all processed data within the Bronze, Silver, and Gold layers.

---

## Objectives

The primary objectives of these conventions are:

- Ensure data consistency and integrity.
- Enable scalable and reliable data processing.
- Improve query performance and optimization.
- Support data governance through Unity Catalog.
- Facilitate auditing and time travel capabilities.
- Standardize data engineering practices across the platform.

---

## Delta Lake Architecture

| Layer | Format | Description |
|-------|--------|-------------|
| Bronze | Delta | Raw structured data ingested from source systems |
| Silver | Delta | Cleansed, standardized, and integrated datasets |
| Gold | Delta | Curated, analytics-ready data models |

All datasets are stored in Delta format to support ACID transactions and advanced data management capabilities.

---

## Table Naming Conventions

| Layer | Naming Pattern | Example |
|-------|----------------|---------|
| Bronze | `bronze_<source>_<entity>` | `bronze_erp_orders` |
| Silver | `silver_<source>_<entity>` | `silver_crm_clients` |
| Gold (Dimensions) | `dim_<entity>` | `dim_customer` |
| Gold (Facts) | `fact_<entity>` | `fact_sales` |
| Gold (Marts) | `mart_<domain>` | `mart_sales_performance` |

---

## Catalog and Schema Standards

| Layer | Catalog | Schema |
|-------|---------|--------|
| Bronze | ptfrozenfoods_dev | bronze |
| Silver | ptfrozenfoods_dev | silver |
| Gold | ptfrozenfoods_dev | gold |

### Example

```sql
SELECT *
FROM ptfrozenfoods_dev.gold.fact_sales;
```

---

## Delta Table Properties

| Property | Purpose |
|----------|---------|
| delta.autoOptimize.optimizeWrite | Optimizes file sizes during writes |
| delta.autoOptimize.autoCompact | Reduces small file issues |
| delta.enableChangeDataFeed | Enables Change Data Feed (optional) |
| delta.columnMapping.mode | Supports schema evolution |

### Example Configuration

```sql
ALTER TABLE ptfrozenfoods_dev.gold.fact_sales
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

---

## Data Optimization Standards

### Auto Optimize and Auto Compaction

These features are enabled to improve performance and reduce maintenance.

```sql
SET spark.databricks.delta.optimizeWrite.enabled = true;
SET spark.databricks.delta.autoCompact.enabled = true;
```

---

### Liquid Clustering

Liquid Clustering is adopted to enhance performance and scalability.

| Table | Clustering Columns |
|-------|--------------------|
| fact_sales | order_date, product_id |
| dim_weather | weather_date |

```sql
ALTER TABLE ptfrozenfoods_dev.gold.fact_sales
CLUSTER BY (order_date, product_id);
```

---

## Schema Evolution

Delta Lake supports schema evolution to accommodate changes in data structures.

```python
df.write \
  .format("delta") \
  .mode("append") \
  .option("mergeSchema", "true") \
  .saveAsTable("ptfrozenfoods_dev.silver.silver_erp_orders")
```

---

## Time Travel and Versioning

Delta Lake enables historical data access and auditing.

```sql
SELECT *
FROM ptfrozenfoods_dev.gold.fact_sales
VERSION AS OF 1;
```

```sql
SELECT *
FROM ptfrozenfoods_dev.gold.fact_sales
TIMESTAMP AS OF '2026-01-01';
```

---

## Change Data Feed (Optional)

```sql
ALTER TABLE ptfrozenfoods_dev.silver.silver_erp_orders
SET TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true'
);
```

---

## Data Quality and Constraints

Delta Lake supports data quality enforcement through constraints.

```sql
ALTER TABLE ptfrozenfoods_dev.gold.fact_sales
ADD CONSTRAINT valid_quantity CHECK (quantity_sold >= 0);
```

---

## Maintenance Operations

### Optimize Tables

```sql
OPTIMIZE ptfrozenfoods_dev.gold.fact_sales;
```

### Vacuum Obsolete Files

```sql
VACUUM ptfrozenfoods_dev.gold.fact_sales RETAIN 168 HOURS;
```

---

## Access Best Practices

- Use Unity Catalog tables instead of direct storage paths.
- Avoid referencing raw ABFSS paths in production workloads.
- Use `saveAsTable()` for managed governance.
- Enforce least-privilege access controls.

---

## Role in the PT Frozen Foods Architecture

Delta Lake is the core storage layer of the Lakehouse architecture.

```
Data Sources
      │
      ▼
Bronze (Delta)
      │
      ▼
Silver (Delta)
      │
      ▼
Gold (Delta)
      │
      ▼
Power BI and Machine Learning
```

---

## Best Practices Implemented

- ACID-compliant transactions.
- Schema enforcement and evolution.
- Centralized governance via Unity Catalog.
- Performance optimization with Liquid Clustering.
- Automated file optimization.
- Time travel and auditing capabilities.
- Secure data access using Managed Identity.
- Scalable design aligned with the Medallion Architecture.

---

## Notes

- All datasets are stored in Delta format.
- Sensitive information is not exposed in this document.
- Configurations follow Microsoft and Databricks best practices.
- Standards may evolve as the platform scales.

---

## References

- https://learn.microsoft.com/azure/databricks/delta/
- https://learn.microsoft.com/azure/databricks/delta/optimization/
- https://learn.microsoft.com/azure/databricks/delta/clustering/
- https://learn.microsoft.com/azure/databricks/lakehouse/
- https://docs.delta.io/latest/index.html
