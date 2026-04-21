### Data Modeling

#### Purpose

This document describes the data modeling approach used in the PT Frozen Foods data platform.

It defines how data is structured to support analytical workloads, Business Intelligence, and future Machine Learning use cases.

---

#### Modeling Approach

The platform follows a dimensional modeling approach based on the Star Schema.

This ensures:

- efficient querying  
- simplified analytics  
- clear business representation  

---

#### Core Concepts

Fact Tables

- store measurable business events  
- contain numeric metrics  
- represent transactional or aggregated data  

Examples:

- fact_sales  

---

Dimension Tables

- provide descriptive context  
- contain attributes used for filtering and grouping  

Examples:

- dim_customer  
- dim_product  
- dim_date  

---

#### Grain Definition

Each fact table must have a clearly defined grain.

Grain represents the level of detail stored in the table.

Example:

    fact_sales → one row per order item

Defining the correct grain ensures:

- accurate aggregations  
- consistent analytics  
- predictable query behavior  

---

#### Relationships

- fact tables connect to dimension tables via keys  
- relationships are typically many-to-one (fact → dimension)  
- joins are based on surrogate or business keys  

Example:

    fact_sales.customer_id → dim_customer.customer_id  

---

#### Surrogate Keys

Where applicable:

- surrogate keys are preferred over natural keys  
- improve join performance  
- ensure consistency across datasets  

---

#### Slowly Changing Dimensions (SCD)

Future implementation may include:

- SCD Type 1 → overwrite changes  
- SCD Type 2 → historical tracking  

---

#### Layer Alignment

The data model is implemented in the Gold layer:

- Bronze → raw ingestion  
- Silver → cleaned and integrated data  
- Gold → dimensional model (facts and dimensions)  

---

#### Data Marts

Marts are built on top of fact and dimension tables.

They provide:

- business-specific views  
- pre-aggregated data  
- optimized datasets for BI  

Example:

- mart_customer_product_mix  

---

#### Design Principles

- simplicity and clarity  
- alignment with business concepts  
- separation of facts and dimensions  
- scalability for future use cases  

---

#### Notes

- current model is optimized for analytical workloads  
- supports Power BI consumption  
- prepared for future ML feature engineering  