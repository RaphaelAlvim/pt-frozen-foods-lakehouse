# Data Model — PT Frozen Foods

## Overview

The analytical data model follows a dimensional approach designed to support reporting, analytics, and future machine learning workloads.

It is centered around business events such as sales transactions and digital interactions, with dimensions providing descriptive context for analysis.

---

## Design Approach

The model is designed to support:

- business analysis and reporting
- dimensional modeling practices
- customer and product analytics
- feature engineering for machine learning
- scalable Gold-layer data products

It aligns with the Lakehouse architecture and is progressively materialized across Bronze, Silver, and Gold layers.

---

## Core Dimensions

### dim_product

Represents product-related attributes.

Typical attributes:

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

Typical attributes:

- customer_id
- customer_name
- customer_segment
- region
- customer_status
- acquisition_date

Future enhancements may include:

- contact information (email, address)
- consent and communication preferences
- Slowly Changing Dimension (SCD) support:
  - effective_start_date
  - effective_end_date
  - is_current

---

### dim_supplier

Represents supplier-related information.

Typical attributes:

- supplier_id
- supplier_name
- supplier_region
- supplier_status

---

### dim_sales_channel

Represents the channel used for sales transactions.

Typical values:

- field_sales
- phone
- ecommerce_b2b
- ecommerce_b2c
- marketplace

---

### dim_calendar

Represents the time dimension for analytical use.

Typical attributes:

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

Represents transactional sales data.

Supports analysis across product, customer, channel, and time.

Typical measures:

- quantity_sold
- gross_sales_amount
- order_count
- line_count

---

### fact_web_events

Represents digital interaction events.

Supports behavioral and funnel analysis.

Typical attributes:

- event_type
- event_timestamp
- session_id
- user_id
- product_id

---

## Gold Analytical Outputs

The model supports curated Gold datasets such as:

- monthly product sales
- channel performance aggregates
- customer RFV metrics
- web session metrics
- funnel metrics
- product interaction metrics

These datasets are designed for both analytics and machine learning preparation.

---

## Modeling Philosophy

The model is designed to balance:

- business readability
- analytical usability
- scalability and extensibility
- alignment with enterprise reporting patterns

It prioritizes practical usability over academic complexity, reflecting real-world data platform needs.