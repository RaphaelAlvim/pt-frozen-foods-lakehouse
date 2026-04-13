# Ingestion Strategy — PT Frozen Foods

## Overview

This document defines how data enters the PT Frozen Foods platform.

The ingestion layer is designed to reflect a realistic enterprise architecture while operating under confidentiality constraints.

Because the project uses synthetic datasets, some sources are manually uploaded, while others are ingested using Azure Logic Apps integrated with SharePoint.

All data lands in the RAW layer before any processing occurs.

---

## Ingestion Principles

The ingestion strategy follows these principles:

- keep ingestion simple and controlled
- separate ingestion from processing
- ensure all data lands first in RAW
- preserve all historical arrivals
- avoid overwriting files
- avoid silent failures
- enable incremental downstream processing

---

## Ingestion Modes

The platform uses two ingestion modes.

### 1. Manual Upload to RAW

Used for datasets that simulate enterprise systems.

Applies to:

- CRM
- ERP
- Weather data
- Web logs

Purpose:

- simulate real-world sources without external dependencies
- keep ingestion controlled and reproducible

---

### 2. SharePoint + Azure Logic Apps

Used for reference datasets.

Applies to:

- reference_calendar
- reference_locations
- reference_sales_channels

Purpose:

- simulate business-managed data sources
- enable event-driven ingestion
- introduce automation in a controlled scope

---

## Source Classification

### Manual Sources

**CRM**
- crm_clients
- crm_segmentation
- crm_status

**ERP**
- erp_orders
- erp_order_items
- erp_products
- erp_salespersons
- erp_suppliers

**Weather**
- weather_porto_daily

**Web**
- web_event_logs

---

### Automated Sources (Logic App)

- reference_calendar
- reference_locations
- reference_sales_channels

---

## Role of the RAW Layer

All data must land in RAW before processing.

RAW is responsible for:

- preserving original files
- isolating ingestion from transformation
- enabling traceability
- supporting reprocessing

No transformation occurs in RAW.

---

## RAW Structure

Data is organized as:

raw/<domain>/<dataset>/load_date=YYYY-MM-DD/

### Example

raw/reference/reference_calendar/load_date=2026-03-19/

### File Naming Convention

<dataset>_yyyyMMddTHHmmssZ.csv

### Rules

- no overwrite
- append-only
- multiple files allowed per day
- full history preserved

---

## File Validation (Logic App)

Reference files are validated by name.

### Accepted

- reference_calendar.csv
- reference_locations.csv
- reference_sales_channels.csv

### Behavior

- valid files → stored in RAW dataset path
- invalid files → stored in the rejected zone

---

## Rejected Files

Invalid files are stored in:

raw/reference/_rejected/load_date=YYYY-MM-DD/

### Rules

- never deleted
- timestamped
- visible for audit and debugging

---

## Notification

Rejected files trigger email alerts.

### Content

- file name
- timestamp (UTC)
- rejection reason
- storage path

---

## Authentication Strategy

### SharePoint

- uses a dedicated service account

### Storage

- uses Managed Identity

### Access Control

- RBAC applied via security groups

---

## Relationship with Processing (Databricks)

Azure Databricks is responsible for processing after ingestion.

### RAW → Bronze

- handled by Auto Loader (cloudFiles)
- incremental ingestion
- schema inference and evolution enabled
- data converted to Delta format
- checkpointing ensures exactly-once processing

### Bronze Characteristics

- one notebook per dataset
- configuration-driven ingestion
- standardized structure across all domains
- technical columns added:
  - ingestion_timestamp
  - source_file

### Key Separation

- ingestion (RAW) → handled outside Databricks
- processing (Bronze and beyond) → handled inside Databricks

---

## Relationship with Azure Data Factory (ADF)

ADF is used for orchestration only.

### Responsibilities

- triggers Databricks notebooks
- manages execution flow
- schedules pipelines

ADF does not:

- ingest data directly
- perform transformations

---

## Why This Strategy

This design balances realism and control.

- manual ingestion simulates enterprise systems
- Logic Apps introduce automation
- RAW preserves all data
- Databricks handles scalable processing
- Azure Data Factory orchestrates execution

---

## Current State

The ingestion and Bronze layers are fully implemented and validated.

### Validated Capabilities

- SharePoint file detection
- file validation
- RAW ingestion
- rejected file handling
- email notification
- timestamp-based versioning
- Auto Loader ingestion across all datasets
- Delta Lake storage in Bronze
- Unity Catalog table registration

### Covered Domains

- CRM
- ERP
- Reference
- Weather
- Web

---

## Future Evolution

Possible improvements include:

- exporting Logic Apps to code
- integrating Logic Apps with Terraform
- enhancing ingestion monitoring
- tracking ingestion metadata
- implementing CI/CD pipelines
- adding validation workflows after ingestion
- implementing data quality checks in the Silver layer