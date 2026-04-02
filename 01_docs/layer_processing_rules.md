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

- ingestion is performed using Auto Loader
- data is stored in Delta format
- schema evolution is enabled
- minimal transformations only
- business logic is not applied
- data is stored using explicit paths in ADLS
- tables are registered in Unity Catalog using LOCATION

### Structure

- storage path:
  bronze/<domain>/<dataset>/

- control paths:
  _checkpoints/<domain>/<dataset>/
  _schemas/<domain>/<dataset>/

### Typical Activities

- read files from RAW using Auto Loader
- infer and evolve schema automatically
- standardize column names (technical format)
- apply minimal data type adjustments if required
- add technical metadata columns:
  - ingestion_timestamp
  - source_file
- write data as Delta files to ADLS
- register table in Unity Catalog

---

## Silver Layer

The Silver layer is the cleaned and integrated layer.

### Purpose

- clean and validate data
- standardize business fields
- integrate datasets
- enrich data with reference information
- create reusable intermediate datasets

### Rules

- data is stored in Delta format
- transformations are business-aware
- joins across datasets are allowed
- duplicates and null values are handled
- outputs must be reusable

### Typical Activities

- apply data quality rules
- deduplicate records
- normalize values
- join transactional and reference data
- create curated datasets for downstream use

---

## Gold Layer

The Gold layer is the analytical delivery layer.

### Purpose

- provide datasets ready for BI and analytics
- support machine learning use cases
- expose business-level metrics

### Rules

- data remains in Delta format
- datasets must be stable and documented
- transformations are business-oriented

### Typical Activities

- create aggregated tables
- build analytical views
- prepare datasets for dashboards
- support forecasting and recommendation models

---

## Processing Philosophy

The processing flow follows:

RAW -> Bronze -> Silver -> Gold

### Layer Responsibilities

- RAW: preserves data as received
- Bronze: performs technical ingestion and standardization
- Silver: performs cleaning and integration
- Gold: delivers business-ready data

### Key Principles

- clear separation of concerns
- traceability across layers
- incremental processing
- scalable architecture
- alignment with enterprise data platform standards