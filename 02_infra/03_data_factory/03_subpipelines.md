### Sub Pipelines

#### Purpose

This document describes the sub-pipelines used in the PT Frozen Foods data platform.

Each sub-pipeline is responsible for a specific layer or processing stage within the Medallion Architecture.

---

### Pipeline List

#### pl_bronze_ingestion

Responsible for ingesting raw data into the Bronze layer.

- source: synthetic datasets
- method: Databricks notebooks (Auto Loader pattern)
- output: Delta tables in bronze schema

---

#### pl_silver_standardization

Responsible for cleaning and standardizing Bronze data.

- input: bronze tables
- transformations: data cleaning, normalization, type casting
- output: standardized datasets in silver schema

---

#### pl_silver_integration

Responsible for integrating and enriching Silver datasets.

- input: standardized silver tables
- transformations: joins, business rules, enrichment logic
- output: integrated datasets ready for analytics

---

#### pl_gold_dimensions

Responsible for building dimension tables.

- input: integrated silver datasets
- output: dimension tables in gold schema
- examples: dim_customer, dim_product

---

#### pl_gold_fact

Responsible for building fact tables.

- input: integrated datasets and dimensions
- output: fact tables in gold schema
- example: fact_sales

---

#### pl_gold_marts

Responsible for building analytical data marts.

- input: fact and dimension tables
- output: business-oriented datasets
- example: mart_customer_product_mix

---

#### Execution Model

- each pipeline executes a specific layer
- pipelines are independent but orchestrated by the master pipeline
- execution order is controlled by pl_ptfrozenfoods_master

---

#### Integration with Databricks

All sub-pipelines execute Databricks notebooks:

- notebooks stored in Git repository
- executed via ADF Databricks activity
- paths aligned with project structure

---

#### Design Principles

- modularization of pipelines
- separation of concerns
- scalability and maintainability
- clear mapping between pipelines and data layers

---

#### Notes

- all pipelines are validated and operational
- pipelines are versioned via Git integration
- structure supports future expansion (BI and ML workflows)