# Data Model — PT Frozen Foods

## Overview

The analytical data model follows a dimensional approach designed to support reporting, analytics, and future machine learning workloads.

It is centered around business events such as sales transactions and digital interactions, with dimensions providing descriptive context for analysis.

The model aligns with modern Lakehouse principles and is implemented using Delta Lake on Azure Databricks, governed by Unity Catalog.

---

## Design Approach

The model is designed to support:

- business analysis and reporting
- dimensional modeling best practices
- customer and product analytics
- feature engineering for machine learning
- scalable Gold-layer data products
- high-performance analytical queries

It aligns with the Lakehouse architecture and is progressively materialized across Raw, Bronze, Silver, and Gold layers.

---

## Medallion Architecture Alignment

| Layer | Purpose | Example |
|-------|---------|---------|
| Raw | Landing zone for source files | `raw/erp/erp_orders.csv` |
| Bronze | Raw technical ingestion with minimal transformation | `bronze_erp_orders` |
| Silver | Cleaned, standardized, and integrated datasets | `silver_orders_customers` |
| Gold | Analytical and dimensional models | `fact_sales`, `dim_customer`, `mart_sales_performance` |

---

## Core Dimensions

### dim_product

Represents product-related attributes.

**Typical attributes:**

- product_id
- product_name
- category
- subcategory
- brand
- package_type
- unit_weight
- status

---

### dim_customer

Represents customer information and segmentation.

**Typical attributes:**

- customer_id
- customer_name
- customer_segment
- region
- customer_status
- acquisition_date

**Future enhancements:**

- contact information (email, address)
- consent and communication preferences
- Slowly Changing Dimension (SCD Type 2):
  - effective_start_date
  - effective_end_date
  - is_current

---

### dim_supplier

Represents supplier-related information.

**Typical attributes:**

- supplier_id
- supplier_name
- supplier_region
- supplier_status

---

### dim_sales_channel

Represents the channel used for sales transactions.

**Typical values:**

- field_sales
- phone
- ecommerce_b2b
- ecommerce_b2c
- marketplace

---

### dim_calendar

Represents the time dimension for analytical use.

**Typical attributes:**

- date
- year
- quarter
- month
- week
- day_of_week
- month_name
- is_weekend

---

## Core Fact Tables

### fact_sales

Represents transactional sales data derived from ERP order items.

Supports analysis across product, customer, channel, and time.

**Typical Measures:**

- quantity_sold
- gross_sales_amount
- net_sales_amount
- order_count
- line_count
- average_order_value

**Primary Key:**

- item_pedido_id — Unique identifier for each order line item.

**Foreign Keys:**

- product_id
- customer_id
- supplier_id
- sales_channel_id
- date

**Grain:**

- One record per order item (`item_pedido_id`).

---

### fact_web_events

Represents digital interaction events.

Supports behavioral analysis and conversion funnel tracking.

**Typical attributes:**

- event_type
- event_timestamp
- session_id
- user_id
- product_id

---

## Silver Layer Dependencies

The Silver layer provides curated datasets that serve as inputs to the Gold layer.

**Key Silver Dataset:**

| Dataset | Description |
|---------|-------------|
| silver_orders_customers | Integrated ERP and CRM data used to build customer-centric analytical models |

This dataset consolidates transactional and customer information, ensuring data quality and referential integrity.

---

## Gold Analytical Outputs

The model supports curated Gold datasets such as:

- monthly product sales
- channel performance aggregates
- customer RFV metrics
- web session metrics
- funnel metrics
- product interaction metrics
- demand forecasting datasets

These datasets are designed for both analytics and machine learning preparation.

---

## Analytics Consumption Layer

The Analytics layer extends the Gold layer by providing consumption-optimized datasets designed for business use cases, BI tools, and recurring analytical queries.

It is not treated as a separate Medallion layer. Instead, it represents a consumption-oriented extension of the Gold layer.

---

### analytics_sales_overview

This dataset was introduced to support multi-dimensional analysis across customer, product, channel, and time.

**Source:**

- `gold.fact_sales`

**Grain:**

- One record per:
  - `data_pedido`
  - `cliente_id`
  - `produto_id`
  - `canal_id`

**Key Dimensions:**

- customer
- product
- sales channel
- calendar attributes

**Key Measures:**

- quantity_sold
- gross_sales_amount
- net_sales_amount
- total_cost_amount
- gross_margin_amount
- order_count
- line_count

**Purpose:**

- enable complex analytical queries without relying on the fact table  
- support BI dashboards and reporting  
- improve query performance and cost efficiency  

**Optimization:**

- Delta Lake format  
- Liquid Clustering applied on:
  - `cliente_id`
  - `produto_id`
  - `canal_id`
- Auto Optimize enabled  

---

### Design Strategy

The Analytics layer follows a usage-driven approach:

- datasets are only created when justified by recurring analytical needs  
- avoids unnecessary duplication of existing marts  
- prioritizes performance and simplicity for data consumption  

Currently, a single dataset is implemented:

- `analytics_sales_overview`

Additional datasets will only be introduced if required by real business use cases or performance constraints.

---

## Storage and Governance

| Component | Specification |
|-----------|---------------|
| Storage Format | Delta Lake |
| Table Type | External Tables |
| Storage Layer | Azure Data Lake Storage Gen2 |
| Governance | Unity Catalog |
| Compute Engine | Azure Databricks |
| Optimization | Liquid Clustering and Auto Optimize |

---

## Modeling Philosophy

The model is designed to balance:

- business readability
- analytical usability
- scalability and extensibility
- performance and optimization
- alignment with enterprise reporting patterns

It prioritizes practical usability over academic complexity, reflecting real-world data platform needs.

---

## Future Enhancements

- implementation of additional fact tables
- adoption of Slowly Changing Dimensions (SCD Type 2)
- semantic modeling for Power BI
- feature stores for machine learning
- data marts for specific business domains
- integration of external data sources
- advanced KPI frameworks

---

## Conclusion

The PT Frozen Foods data model provides a scalable and enterprise-ready analytical foundation.

It ensures:

- consistency and reliability across datasets
- alignment with dimensional modeling best practices
- readiness for BI, analytics, and machine learning
- seamless integration with the Lakehouse architecture
- support for consumption-optimized analytics datasets

This model enables efficient data-driven decision-making and supports the long-term evolution of the platform.