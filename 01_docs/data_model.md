# Data Model — PT Frozen Foods

## Overview

The analytical data model follows a dimensional approach designed to support reporting, analytics, and future machine learning workloads.

The model is centered around business events such as sales transactions and digital interactions, while dimensions provide descriptive context for analysis.

## Design Approach

The data model is structured to support:

- business analysis
- dimensional reporting
- customer and product analytics
- downstream feature engineering
- future serving of Gold datasets for machine learning

The model is aligned with the Lakehouse architecture and intended to be materialized progressively across Bronze, Silver, and Gold layers.

## Core Dimensions

### dim_product

Represents the product catalog and descriptive product attributes.

Typical attributes may include:

- product_id
- product_name
- category
- subcategory
- brand
- package_type
- unit_weight
- status

### dim_customer

Represents customer-level business and relationship information.

Current versions may include core identification and segmentation fields. Future enhancements may expand this dimension to support richer CRM scenarios, including customer contact information, historical tracking, and Slowly Changing Dimension behavior.

Typical attributes may include:

- customer_id
- customer_name
- customer_segment
- region
- customer_status
- acquisition_date

Planned future enhancements may include:

- email
- address fields
- consent flags
- preferred contact channel
- SCD-support columns such as effective_start_date, effective_end_date, and is_current

### dim_supplier

Represents supplier information associated with products and procurement-related analysis.

Typical attributes may include:

- supplier_id
- supplier_name
- supplier_region
- supplier_status

### dim_sales_channel

Represents the sales channel through which transactions occur.

Typical values may include:

- field sales
- phone
- e-commerce B2B
- e-commerce B2C
- marketplace

### dim_calendar

Represents the standard calendar dimension used for time-based analysis.

Typical attributes may include:

- date
- year
- quarter
- month
- week
- day_of_week
- month_name
- is_weekend

## Core Fact Tables

### fact_sales

Represents transactional sales activity.

This table supports analysis across product, customer, channel, and time dimensions.

Typical measures may include:

- quantity_sold
- gross_sales_amount
- order_count
- line_count

### fact_web_events

Represents web interaction events.

This table supports behavioral and funnel analysis.

Typical measures and attributes may include:

- event_type
- event_timestamp
- session_id
- user_id
- product_id
- event_count

## Gold Analytical Outputs

The model also supports curated Gold-layer datasets, including:

- monthly product sales aggregates
- monthly channel sales aggregates
- customer-level RFV metrics
- web session metrics
- funnel metrics
- product interaction metrics
- user behavior metrics

These Gold datasets are intended to support both business analytics and machine learning preparation.

## Modeling Philosophy

The model is intentionally designed to balance:

- business readability
- analytical usefulness
- future extensibility
- alignment with enterprise reporting patterns

It is not intended to be a purely academic schema, but rather a practical dimensional model that reflects real-world business needs within a modern Lakehouse platform.
