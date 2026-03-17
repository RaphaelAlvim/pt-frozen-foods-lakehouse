# Layer Processing Rules — PT Frozen Foods

## Overview

This document defines the role and expected responsibilities of each data layer in the PT Frozen Foods Lakehouse architecture.

The objective is to clearly separate landing, technical structuring, business transformation, and analytical delivery.

---

## RAW Layer

The RAW layer is the landing zone of the platform.

Its purpose is to:

- receive incoming files exactly as they arrive
- preserve original file format and structure
- isolate ingestion from transformation
- support traceability and reprocessing

Rules for RAW:

- files remain in their original format
- no business transformation is applied
- no analytical modeling is performed
- no conversion to Delta Lake happens in RAW

Examples:
- csv remains csv
- source-aligned file naming is preserved
- files are organized by source domain, dataset, and load_date

---

## Bronze Layer

The Bronze layer is the first transformation layer after RAW.

Its purpose is to:

- read incoming files from RAW
- apply technical standardization
- perform initial schema handling
- convert the data into Delta Lake format
- preserve source-level granularity

Rules for Bronze:

- data is converted to Delta Lake
- initial typing may be applied
- ingestion metadata may be added
- source structure is preserved as much as possible
- business logic should remain minimal

Typical Bronze activities:

- read csv/json files from RAW
- standardize column names if required
- add technical columns such as load_date or ingestion_timestamp
- write output as Delta tables/files

---

## Silver Layer

The Silver layer is the cleaned and integrated layer.

Its purpose is to:

- clean and validate data
- resolve structural inconsistencies
- enrich datasets
- integrate data across sources
- prepare reusable business-ready intermediate datasets

Rules for Silver:

- data is stored in Delta Lake format
- transformations become business-aware
- joins across datasets are allowed
- duplicate handling and null treatment may occur
- reusable curated tables are produced

Typical Silver activities:

- standardize business fields
- apply deduplication rules
- perform joins between transactional and reference data
- enrich transactional data with descriptive dimensions
- create stable datasets for Gold and feature engineering

---

## Gold Layer

The Gold layer is the analytical delivery layer.

Its purpose is to:

- provide curated datasets for reporting and analytics
- support machine learning preparation
- expose business-friendly tables and aggregates

Rules for Gold:

- data remains in Delta Lake format
- datasets must be ready for consumption
- transformations are business-oriented
- outputs must be stable and clearly documented

Typical Gold activities:

- create aggregated analytical tables
- create customer-level analytical views
- create product/channel performance outputs
- prepare datasets for recommendation and forecasting

---

## Processing Philosophy

The processing flow follows this logic:

RAW -> Bronze -> Silver -> Gold

This means:

- RAW preserves arrival
- Bronze standardizes technically and converts to Delta
- Silver cleans and integrates
- Gold delivers business-ready outputs

This separation improves maintainability, traceability, and reusability across the platform.