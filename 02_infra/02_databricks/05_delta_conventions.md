### Delta Lake Conventions — PT Frozen Foods

#### Overview

This document defines the Delta Lake conventions adopted in the PT Frozen Foods data platform.

Delta Lake is the standard storage format used across all layers (Bronze, Silver, Gold), ensuring consistency, performance, and governance within the Lakehouse architecture.

---

#### Core Principles

- All datasets are stored in Delta format
- Unity Catalog is used for table access and governance
- Direct storage path access is avoided in production logic
- Tables are managed through catalog and schema definitions

---

#### Layer Standards

| Layer | Format | Purpose |
|-------|--------|--------|
| Bronze | Delta | Raw structured ingestion |
| Silver | Delta | Cleansed and integrated data |
| Gold | Delta | Analytical models and marts |

---

#### Naming Conventions

| Layer | Pattern | Example |
|------|--------|--------|
| Bronze | bronze_<source>_<entity> | bronze_erp_orders |
| Silver | silver_<source>_<entity> | silver_crm_clients |
| Gold Dimensions | dim_<entity> | dim_customer |
| Gold Facts | fact_<entity> | fact_sales |
| Gold Marts | mart_<domain> | mart_sales_performance |

---

#### Catalog and Schema

| Layer | Catalog | Schema |
|------|---------|--------|
| Bronze | ptfrozenfoods_dev | bronze |
| Silver | ptfrozenfoods_dev | silver |
| Gold | ptfrozenfoods_dev | gold |

Example:

SELECT * FROM ptfrozenfoods_dev.gold.fact_sales;

---

#### Table Configuration

The following Delta properties are applied:

- delta.autoOptimize.optimizeWrite = true
- delta.autoOptimize.autoCompact = true

These settings ensure efficient file sizes and reduce small file issues.

---

#### Optimization Strategy

##### Auto Optimization

Enabled at table level to improve write performance and storage layout.

##### Liquid Clustering

Used in analytical tables (Gold layer) to improve query performance.

Example:

ALTER TABLE ptfrozenfoods_dev.gold.fact_sales
CLUSTER BY (order_date, product_id);

---

#### Write Patterns

- Tables are written using Delta format
- Overwrite is used for full refresh datasets
- Append is used for incremental ingestion where applicable
- Schema evolution is controlled and used only when necessary

---

#### Data Access

- All access is done through Unity Catalog tables
- Direct ABFSS paths are not used in production notebooks
- Access control is enforced via GRANT statements

---

#### Performance Considerations

- Avoid unnecessary full scans
- Minimize expensive operations in processing notebooks
- Use clustering strategically in large tables
- Keep processing logic optimized for cost and performance

---

#### Maintenance

Basic maintenance operations may be applied when needed:

OPTIMIZE ptfrozenfoods_dev.gold.fact_sales;

VACUUM operations are not automated at this stage.

---

#### Role in the Architecture

Delta Lake is the storage foundation of the platform:

Bronze → Silver → Gold → BI / ML

It enables:

- reliable data processing
- versioned storage
- consistent table management

---

#### Notes

- Not all Delta features are used in this project
- The focus is on practical and production-relevant configurations
- Conventions may evolve as the platform matures

---

#### Conclusion

Delta Lake conventions ensure consistency, performance, and maintainability across the platform, supporting a scalable and production-ready data architecture.
