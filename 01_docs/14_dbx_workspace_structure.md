# Workspace Structure — PT Frozen Foods

## Overview

This document describes the organization of the Databricks workspace used in the PT Frozen Foods data platform.

The structure is aligned with the repository layout and follows a layered architecture approach based on the Medallion Architecture.

---

## Source of Truth

The workspace structure is **not manually maintained**.

It is derived directly from the Git repository through Databricks Git folders.

Key principles:

- The **Git repository is the single source of truth**
- The **Databricks Workspace reflects the repository structure**
- No notebooks should exist outside the Git folder as official project artifacts

---

## Workspace Root Structure

pt-frozen-foods-lakehouse/

├── 00_setup/  
├── 01_source/  
├── 02_bronze/  
├── 03_silver/  
├── 04_gold/  
├── 05_analytics/  
├── 06_ml/  

> Note:
> The previous `07_utils` layer has been deprecated.
> Utility code is now managed in `05_src/` within the repository.

---

## Layer Description

### 00_setup

Contains environment and governance setup logic.

Includes:

- Unity Catalog setup  
- catalog creation  
- schema creation  
- permissions and grants  
- environment bootstrap scripts  

Important:

- Maintained in the repository under:  
  `02_infra/02_databricks/00_setup/`
- Not considered business notebooks  

---

### 01_source

Reserved for exploratory analysis of raw data sources.

Used for:

- data inspection  
- schema validation  
- initial exploration  

---

### 02_bronze

Contains ingestion notebooks.

Structure:

- ingestion/<domain>/<dataset>

Each dataset includes:

- one notebook per dataset  
- Auto Loader ingestion  
- explicit Delta path definition  
- Unity Catalog table registration  

---

### 03_silver

Contains data cleaning and transformation logic.

Structure:

- processing/  
- exploratory/  

Responsibilities:

- data standardization  
- data cleansing  
- integration across domains  

---

### 04_gold

Contains business-level datasets and analytical outputs.

Structure:

- processing/  
  - dimensions/  
  - facts/  
  - marts/  
- exploratory/  

Important:

- Gold layer follows a dimensional modeling approach  
- `fact_sales` is an enriched (wide) fact table  
- Marts are built primarily from the fact table to avoid unnecessary joins  

---

### 05_analytics

Reserved for business analysis and reporting logic.

Examples:

- KPI calculations  
- business logic for reporting  
- preparation for BI tools  

---

### 06_ml

Reserved for machine learning workflows.

Examples:

- feature engineering  
- model training  
- inference pipelines  

---

## Naming Conventions

### Folders

Numeric prefixes represent logical execution order:

- 00_setup  
- 01_source  
- 02_bronze  
- 03_silver  
- 04_gold  
- 05_analytics  
- 06_ml  

---

### Bronze Notebooks

Pattern:

bronze_<domain>_<dataset>_autoloader

Example:

bronze_reference_locations_autoloader

---

### Silver Notebooks

Pattern:

silver_<domain>_<dataset>

---

### Gold Notebooks

Dimensions:

gold_dim_<entity>

Facts:

gold_fact_<entity>

Marts:

gold_mart_<business_view>

---

## Design Principles

- Git repository as the single source of truth  
- Workspace as a reflection of the repository  
- Clear separation between infrastructure and business logic  
- Layered architecture following Medallion principles  
- High cohesion within layers and low coupling across layers  
- Avoidance of redundant data processing  
- Performance-first approach in processing notebooks  
- Reproducibility through version-controlled artifacts  

---

## Operational Guidelines

- All development must originate from the repository  
- Changes must be committed and pushed before execution validation  
- Databricks Git folders must be used for synchronization  
- ADF pipelines must reference only repository-aligned paths  
- No production notebook should exist outside the Git folder structure  

---

## Conclusion

The Workspace structure is no longer an independent artifact.

It is a direct projection of the repository structure and must be treated as such.

This ensures:

- consistency across environments  
- reproducibility  
- maintainability  
- alignment with modern data platform engineering practices  