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
4. development and debugging support (DEV ONLY)

This separation ensures clarity, maintainability, and alignment with enterprise practices.

---

## Bronze Notebooks (Ingestion Layer)

Bronze notebooks are responsible for ingesting data from RAW into Delta format.

### Purpose

- read data from RAW
- perform incremental ingestion using Auto Loader
- handle schema evolution
- apply minimal technical transformations
- write data to Bronze layer in ADLS
- register tables in Unity Catalog

### Rules

- one notebook per dataset
- ingestion must be incremental
- use Auto Loader (cloudFiles)
- use explicit storage paths in ADLS
- schema evolution must be enabled
- no business logic allowed
- only technical transformations

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

Silver notebooks are responsible for cleaning and integrating data.

### Purpose

- clean data
- standardize business fields
- apply validation rules
- integrate datasets
- prepare reusable datasets

### Rules

- business-aware transformations allowed
- joins across datasets allowed
- deduplication allowed
- null handling required
- outputs must be reusable
- schema must be controlled

---

## Gold Notebooks (Analytics Layer)

Gold notebooks deliver business-ready datasets.

### Purpose

- create analytical tables
- provide datasets for BI and ML
- expose metrics and aggregations

### Rules

- outputs must be stable
- logic must be business-oriented
- datasets must be documented

---

## Setup and Governance Notebooks

These notebooks configure the environment and governance.

### Examples

- create catalog
- create schemas
- manage permissions
- audit external locations

### Rules

- executed manually or during setup
- not part of orchestration pipelines
- must be versioned

---

## DEV ONLY / DEBUG Section

Some notebooks may include a DEV section for validation.

### Purpose

- inspect results
- validate schema
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

- interactive (development)
- job clusters (production)

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
5. orchestrate with ADF when required

### Silver and Gold

1. define transformation logic
2. implement notebook
3. validate outputs
4. version in GitHub
5. orchestrate with ADF

---

## Relationship with ADF

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
- setup notebooks

Temporary notebooks do not need to be preserved.

---

## Practical Rule

- use Databricks to build and validate
- use GitHub to version and document
- use ADF to orchestrate

This ensures a clean, scalable, and enterprise-ready platform.