# Layer Processing Rules — PT Frozen Foods

## Overview

This document defines the role and responsibilities of each data layer in the PT Frozen Foods Lakehouse architecture.

The objective is to clearly separate:

- data landing
- technical structuring
- business transformation
- analytical delivery

---

## RAW Layer

The RAW layer is the landing zone of the platform.

### Purpose

- receive incoming files exactly as they arrive
- preserve original file format and structure
- isolate ingestion from transformation
- support traceability and reprocessing

### Rules

- files remain in original format
- no transformation is applied
- no schema changes are applied
- no conversion to Delta Lake
- ingestion must be append-only (no overwrite)

### Structure

- organized by domain, dataset, and load_date
- example:
  raw/<domain>/<dataset>/load_date=YYYY-MM-DD/

---

## Bronze Layer

The Bronze layer is the first processing layer after RAW.

### Purpose

- ingest data incrementally from RAW
- apply technical standardization
- handle schema evolution
- convert data to Delta Lake
- preserve source-level granularity

### Rules

- ingestion is performed using Auto Loader (cloudFiles)
- data is stored in Delta format
- schema evolution is enabled
- minimal transformations only
- business logic is not applied
- data is stored using explicit paths in ADLS Gen2
- tables are registered in Unity Catalog using LOCATION
- ingestion must be idempotent and incremental
- tables are implemented as external Delta tables

### Structure

- storage path:
  bronze/<domain>/<dataset>/

- control paths:
  _checkpoints/<domain>/<dataset>/
  _schemas/<domain>/<dataset>/

### Typical Activities

- read files from RAW using Auto Loader
- infer and evolve schema automatically
- apply minimal technical normalization if required
- add technical metadata columns:
  - ingestion_timestamp
  - source_file
- write data as Delta files to ADLS
- register tables in Unity Catalog

### Current Status

The Bronze layer is fully implemented across all domains:

- CRM
- ERP
- Reference
- Weather API
- Web

---

## Silver Layer

The Silver layer is the cleaned, standardized, and integrated layer.

### Purpose

- clean and validate data
- standardize business fields
- integrate datasets across domains
- enrich data with reference information
- create reusable intermediate datasets

### Rules

- data is stored in Delta format
- tables are implemented as external Delta tables
- transformations are business-aware
- joins across datasets are allowed
- duplicates and null values are handled
- outputs must be reusable and stable
- schema must be controlled and explicit
- datasets must be governed by Unity Catalog

### Silver Sub-Layers

#### Curated Silver
- standardized datasets derived from a single source
- prepared for integration and reuse

#### Silver Integration
- integrates datasets from multiple domains
- supports cross-functional analytics
- serves as the foundation for the Gold layer

Example:
- `silver_orders_customers` (ERP + CRM integration)

### Typical Activities

- apply data quality rules
- deduplicate records
- normalize values
- cast data types
- join transactional and reference data
- create curated and integrated datasets
- implement performance optimizations

### Optimization Standards

- Delta Lake storage format
- external tables on ADLS Gen2
- Liquid Clustering for optimized data layout
- Auto Optimize for efficient writes and compaction
- Photon engine acceleration
- Adaptive Query Execution (AQE)
- column pruning and optimized join strategies

### Current Status

The Silver layer is implemented and includes curated and integrated datasets aligned with business requirements.

---

## Gold Layer

The Gold layer is the analytical delivery layer.

### Purpose

- provide datasets ready for BI and analytics
- support machine learning use cases
- expose business-level metrics

### Rules

- data remains in Delta format
- datasets must be stable, documented, and governed
- transformations are business-oriented
- outputs should be optimized for consumption
- tables follow dimensional modeling principles
- datasets are optimized for query performance

### Typical Activities

- create fact and dimension tables
- implement star and snowflake schemas
- create aggregated tables
- build analytical views
- prepare datasets for dashboards
- support forecasting and recommendation models

---

## Processing Philosophy

The processing flow follows:

RAW → Bronze → Silver → Gold

### Layer Responsibilities

- **RAW:** preserves data as received
- **Bronze:** performs technical ingestion and structuring
- **Silver:** performs cleaning, validation, and integration
- **Gold:** delivers business-ready datasets

### Key Principles

- clear separation of concerns
- traceability across layers
- incremental processing
- scalable and modular architecture
- governed data access through Unity Catalog
- alignment with enterprise Lakehouse standards
- optimization for performance and scalability