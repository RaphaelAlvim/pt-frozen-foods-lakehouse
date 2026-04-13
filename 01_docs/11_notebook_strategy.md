# Notebook Strategy — PT Frozen Foods

## Overview

This document defines how notebooks are used in the PT Frozen Foods platform.

The objective is to clearly separate:

- ingestion logic
- transformation logic
- development and validation activities

The platform uses Azure Databricks as the execution environment and GitHub for version control.

---

## General Principle

Not all notebooks serve the same purpose.

In this project, notebooks are divided into:

1. ingestion notebooks (Bronze)
2. transformation notebooks (Silver and Gold)
3. setup and governance notebooks
4. exploratory, development, and debugging support (DEV ONLY)

This separation ensures clarity, maintainability, and alignment with enterprise practices.

---

## Bronze Notebooks (Ingestion Layer)

Bronze notebooks are responsible for ingesting data from RAW into Delta format.

### Purpose

- read data from RAW
- perform incremental ingestion using Auto Loader
- handle schema evolution
- apply minimal technical transformations
- write data to the Bronze layer in ADLS
- register tables in Unity Catalog

### Rules

- one notebook per dataset
- ingestion must be incremental
- use Auto Loader (cloudFiles)
- use explicit storage paths in ADLS
- schema evolution must be enabled
- no business logic allowed
- only technical transformations
- tables must be created as external Delta tables

### Naming Convention

bronze_<domain>_<dataset>_autoloader

### Examples

- bronze_crm_clients_autoloader
- bronze_erp_orders_autoloader
- bronze_reference_locations_autoloader
- bronze_weather_api_weather_porto_daily_autoloader
- bronze_web_web_event_logs_autoloader

### Typical Structure

Each Bronze notebook should include:

1. configuration (centralized at the top)
2. context setup (catalog and schema)
3. configuration summary (logs)
4. pre-checks (paths and access)
5. Auto Loader ingestion logic
6. technical columns:
   - ingestion_timestamp
   - source_file
7. write to Delta (explicit path)
8. table registration in Unity Catalog
9. final execution status

### Current Status

The Bronze layer is fully implemented and standardized across all domains.

---

## Silver Notebooks (Processing Layer)

Silver notebooks are responsible for cleaning, standardizing, and integrating data.

### Purpose

- clean data
- standardize business fields
- apply validation rules
- integrate datasets
- prepare reusable datasets

### Rules

- business-aware transformations are allowed
- joins across datasets are permitted
- deduplication and null handling are required
- outputs must be reusable and stable
- schemas must be controlled and explicit
- data must be stored in Delta format
- tables must be created as external Delta tables
- datasets must be governed by Unity Catalog

### Silver Sub-Layers

#### Curated Silver
- standardized datasets derived from a single source
- prepared for reuse and integration

#### Silver Integration
- integrates datasets from multiple domains
- supports cross-functional analytics
- serves as the foundation for the Gold layer

### Naming Conventions

- Curated Silver:
  silver_<domain>_<dataset>

- Silver Integration:
  silver_integration_<dataset>

### Example

- silver_erp_orders
- silver_crm_clients
- silver_integration_orders_customers

### Optimization Standards

Silver notebooks may implement modern Lakehouse optimizations:

- Delta Lake format
- column pruning
- optimized join strategies
- broadcast joins for small datasets
- Auto Optimize (optimizeWrite and autoCompact)
- Liquid Clustering for performance optimization
- Photon engine acceleration

---

## Gold Notebooks (Analytics Layer)

Gold notebooks deliver business-ready datasets.

### Purpose

- create analytical tables
- provide datasets for BI and ML
- expose metrics and aggregations
- implement dimensional models

### Rules

- outputs must be stable and documented
- logic must be business-oriented
- datasets must follow dimensional modeling principles
- tables must be optimized for query performance
- data must remain in Delta format

### Typical Outputs

- fact tables
- dimension tables
- aggregated datasets
- KPI models
- semantic-ready datasets for Power BI

---

## Setup and Governance Notebooks

These notebooks configure the environment and governance.

### Examples

- create catalog
- create schemas
- manage permissions
- audit external locations
- configure storage credentials

### Rules

- executed manually or during setup
- not part of orchestration pipelines
- must be versioned

---

## Exploratory Notebooks (DEV ONLY)

Exploratory notebooks are used for data understanding and validation.

### Purpose

- perform data profiling and analysis
- validate schemas and joins
- detect anomalies and duplicates
- support processing design decisions

### Naming Convention

exploratory_<layer>_<dataset>

### Example

- exploratory_silver_orders_customers

### Rules

- must not be orchestrated
- must not write production data
- used only during development
- must be versioned for traceability

---

## DEV ONLY / DEBUG Section

Some notebooks may include a DEV section for validation.

### Purpose

- inspect results
- validate schemas
- debug ingestion issues

### Examples

- preview data
- count rows
- inspect schema
- check duplicates
- stop streaming queries

### Rules

- must be clearly marked as DEV ONLY
- must not affect production logic
- should be removed or disabled in production

---

## Execution Environment

Azure Databricks is used to:

- access ADLS
- execute ingestion and transformations
- test and validate pipelines

Clusters can be:

- interactive clusters (development)
- job clusters (production)
- serverless compute (when available)

---

## Role of GitHub and VS Code

GitHub is used for version control.

VS Code is used for organizing and reviewing code locally.

### Workflow

- develop and test notebooks in Databricks
- export and version notebooks in GitHub
- organize project structure in VS Code

---

## Recommended Workflow

### Bronze

1. define dataset ingestion
2. implement Auto Loader notebook
3. validate ingestion in Databricks
4. version notebook in GitHub
5. orchestrate with Azure Data Factory when required

### Silver and Gold

1. define transformation logic
2. implement notebook
3. validate outputs
4. version in GitHub
5. orchestrate with Azure Data Factory

---

## Relationship with Azure Data Factory (ADF)

ADF is responsible for orchestration.

ADF should:

- trigger Bronze ingestion notebooks
- trigger Silver transformations
- trigger Gold pipelines

ADF should not:

- perform transformations directly
- be used for exploration

---

## Layer Alignment

The notebook strategy follows the platform layers:

### RAW
- no transformation notebooks

### Bronze
- ingestion notebooks only
- Auto Loader
- Delta conversion

### Silver
- cleaning and integration notebooks

### Gold
- analytical notebooks

---

## Versioning Principle

All production notebooks must be versioned.

This includes:

- Bronze notebooks
- Silver notebooks
- Gold notebooks
- setup and governance notebooks
- exploratory notebooks supporting design decisions

Temporary notebooks do not need to be preserved.

---

## Practical Rule

- use Databricks to build and validate
- use GitHub to version and document
- use Azure Data Factory to orchestrate

This ensures a clean, scalable, and enterprise-ready platform.