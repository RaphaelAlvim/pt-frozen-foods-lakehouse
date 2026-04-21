### Naming Conventions

#### Purpose

This document defines the naming conventions used in the PT Frozen Foods data platform.

It ensures consistency, readability, and standardization across all data layers and SQL objects.

---

#### General Principles

- use lowercase for all object names  
- use underscores (_) to separate words  
- avoid spaces and special characters  
- use clear and descriptive names  

---

#### Layer-Based Naming

Tables follow naming patterns based on the Medallion Architecture.

Bronze

    bronze_<source>_<entity>

Example:

    bronze_erp_orders  

---

Silver

    silver_<source>_<entity>

Example:

    silver_crm_clients  

---

Gold (Dimensions)

    dim_<entity>

Example:

    dim_customer  
    dim_product  

---

Gold (Facts)

    fact_<entity>

Example:

    fact_sales  

---

Gold (Marts)

    mart_<domain>

Example:

    mart_customer_product_mix  

---

#### Column Naming

- use lowercase  
- use underscores  
- use descriptive names  

Examples:

    customer_id  
    order_date  
    total_sales  

---

#### Key Naming

Primary Keys:

    <entity>_id

Examples:

    customer_id  
    product_id  

Foreign Keys:

    <referenced_entity>_id

Examples:

    customer_id  
    product_id  

---

#### Timestamp Columns

Use standardized naming:

    created_at  
    updated_at  

---

#### Boolean Columns

Use clear prefixes:

    is_active  
    is_deleted  

---

#### Avoid

- abbreviations that are not obvious  
- inconsistent naming patterns  
- mixing naming styles  

---

#### Consistency Across Layers

- maintain consistent naming from Bronze to Gold  
- avoid renaming fields unnecessarily  
- ensure traceability across layers  

---

#### Notes

- conventions apply to all tables and columns  
- aligned with analytical and BI best practices  
- designed for scalability and maintainability  