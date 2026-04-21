### Data Paths

#### Purpose

This document describes the physical storage paths used in the PT Frozen Foods data platform.

It defines how data is organized in Azure Data Lake Storage (ADLS) across all layers.

---

#### Storage Structure

The platform uses container-based separation aligned with the Medallion Architecture:

    raw
    bronze
    silver
    gold

Each layer is mapped to a dedicated container.

---

#### Base Paths

All data is stored using ABFSS protocol:

    abfss://<container>@<storage_account>.dfs.core.windows.net/

---

#### RAW Layer

Container:

    raw

Structure:

    raw/<source>/<dataset>

Examples:

    raw/crm/crm_clients.csv
    raw/erp/erp_orders.csv
    raw/reference/reference_calendar.csv

---

#### Bronze Layer

Container:

    bronze

Structure:

    bronze/<domain>/<dataset>

Examples:

    bronze/erp/orders/
    bronze/crm/clients/

---

#### Silver Layer

Container:

    silver

Structure:

    silver/<domain>/<dataset>

Examples:

    silver/erp/orders/
    silver/crm/clients/

---

#### Gold Layer

Container:

    gold

Structure:

    gold/<type>/<dataset>

Where:

    type = dim | fact | mart

Examples:

    gold/dim/customer/
    gold/fact/sales/
    gold/mart/customer_product_mix/

---

#### Partitioning Strategy

Where applicable, data is partitioned by:

- date  
- domain-specific keys  

Example:

    gold/fact/sales/order_date=2025-01-01/

---

#### Reference Data (Logic App)

Reference data ingestion follows a structured path:

    raw/reference/<dataset>/load_date=YYYY-MM-DD/<file>

Rejected files:

    raw/reference/_rejected/load_date=YYYY-MM-DD/<file>

---

#### Integration with Unity Catalog

All paths are abstracted via Unity Catalog:

- tables are accessed via catalog.schema.table  
- direct path usage is minimized in production  

---

#### Design Principles

- separation by container  
- consistent folder structure  
- traceability across layers  
- support for incremental processing  

---

#### Notes

- paths are managed via External Locations  
- access is controlled via Unity Catalog  
- structure is scalable and production-ready  