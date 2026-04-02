# Ingestion Strategy — PT Frozen Foods

## Overview

This document defines how data enters the PT Frozen Foods platform.

The ingestion layer is designed to reflect a realistic enterprise architecture while operating under confidentiality constraints.

Because the project uses synthetic datasets, some sources are manually uploaded, while others are ingested using Azure Logic Apps integrated with SharePoint.

All data lands in RAW before any processing occurs.

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
- weather data
- web logs

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

- CRM:
  - crm_clients
  - crm_segmentation
  - crm_status

- ERP:
  - erp_orders
  - erp_order_items
  - erp_products
  - erp_salespersons
  - erp_suppliers

- Weather:
  - weather_porto_daily

- Web:
  - web_event_logs

---

### Automated Sources (Logic App)

- reference_calendar
- reference_locations
- reference_sales_channels

---

## Role of RAW Layer

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

### File Naming

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

- valid → stored in RAW dataset path
- invalid → stored in rejected zone

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

- uses dedicated service account

### Storage

- uses Managed Identity

### Access Control

- RBAC applied via security groups

---

## Relationship with Processing (Databricks)

Azure Databricks is responsible for processing after ingestion.

### RAW → Bronze

- handled by Auto Loader
- incremental ingestion
- schema evolution enabled
- data converted to Delta format

### Key Separation

- ingestion (RAW) → handled outside DBX
- processing (Bronze+) → handled inside DBX

---

## Relationship with ADF

ADF is used for orchestration only.

ADF:

- triggers Databricks notebooks
- manages execution flow

ADF does not:

- ingest data directly
- perform transformations

---

## Why This Strategy

This design balances realism and control.

- manual ingestion simulates enterprise systems
- Logic App introduces automation
- RAW preserves all data
- DBX handles scalable processing
- ADF orchestrates execution

---

## Current State

The ingestion flow is operational and validated.

Validated capabilities:

- SharePoint file detection
- file validation
- RAW ingestion
- rejected file handling
- email notification
- timestamp-based versioning

---

## Future Evolution

Possible improvements:

- export Logic App to code
- integrate with Terraform
- add ingestion monitoring
- track ingestion metadata
- implement CI/CD
- add validation workflows after ingestion

