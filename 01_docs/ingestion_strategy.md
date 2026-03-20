# Ingestion Strategy — PT Frozen Foods

## Overview

This document defines how data enters the PT Frozen Foods platform.

The ingestion layer is designed to reflect a realistic enterprise architecture while operating under confidentiality constraints. Because the project uses synthetic datasets and a fictional company name, some data sources are manually loaded into the RAW zone, while selected sources are ingested through Azure Logic Apps integrated with SharePoint.

This approach preserves architectural realism while keeping the implementation controlled, reproducible, and compatible with Infrastructure as Code and future CI/CD practices.

---

## Ingestion Principles

The ingestion strategy follows these principles:

- keep the landing process simple and controlled
- separate ingestion from orchestration and processing
- ensure all data first lands in the RAW layer
- use automated ingestion only where it adds architectural value
- use manual ingestion where synthetic data and confidentiality constraints make direct source integration unnecessary
- preserve historical arrivals in RAW
- avoid silent failures during ingestion

---

## Ingestion Modes

The platform uses two ingestion modes:

### 1. Manual upload to ADLS RAW

Manual upload is used for datasets that represent enterprise operational systems but are implemented with synthetic data.

This applies to:

- CRM datasets
- ERP datasets
- weather data
- web event data

The goal is to represent business data arriving from systems such as CRM, ERP platforms, APIs, and operational logs without exposing or depending on real production systems.

### 2. SharePoint + Azure Logic Apps

Azure Logic Apps integrated with SharePoint is used for selected reference datasets.

This applies to:

- reference_calendar
- reference_sales_channels
- reference_locations

These datasets are suitable for SharePoint-driven ingestion because they are smaller, more stable, and closer to business-managed reference files.

---

## Source Classification

### Manual ingestion sources

#### CRM
- crm_clients
- crm_segmentation
- crm_status

#### ERP
- erp_suppliers
- erp_order_items
- erp_orders
- erp_products
- erp_salespersons

#### Weather API
- weather_porto_daily

#### Web
- web_event_logs

### Logic App + SharePoint sources

#### Reference
- reference_calendar
- reference_sales_channels
- reference_locations

---

## Role of the RAW Layer

All data must first land in the RAW layer of ADLS Gen2.

The RAW layer is the controlled landing zone of the platform and is responsible for:

- preserving the original structure of incoming files
- isolating ingestion from transformation
- enabling traceability of source arrivals
- supporting repeatable Bronze processing
- preserving historical versions of arrivals

No business transformations should occur in the RAW layer.

---

## Relationship with ADF and Databricks

Azure Data Factory is not the primary ingestion engine in this project.

ADF is used as the orchestration layer after data is already available in RAW.

Its role is to:

- trigger Databricks notebooks
- coordinate processing across Bronze, Silver, and Gold
- manage execution dependencies

Azure Databricks is responsible for transformation and processing after ingestion is complete.

---

## SharePoint + Logic App Design

The automated ingestion design for reference files is based on a SharePoint folder monitored by a single Azure Logic App.

### SharePoint source

- tenant host: `rmdatascience.sharepoint.com`
- site path: `/sites/PT-Frozen-Foods-Data`
- site name: `PT-Frozen-Foods-Data`
- document library: `Documents`
- monitored folder: `reference`

### Files monitored

- `reference_calendar.csv`
- `reference_locations.csv`
- `reference_sales_channels.csv`

### Trigger behavior

The Logic App uses a trigger equivalent to:

- when a file is created or modified in the monitored folder

This ensures that both new files and updates to existing files are captured and ingested.

### Trigger frequency

The SharePoint trigger works with polling, not with native push events.

For this project:

- polling frequency can be adjusted depending on operational needs
- lower frequency is acceptable because reference files are low volume
- shorter polling intervals may be used temporarily during testing

### Logic App design choice

A single Logic App is used for all reference files.

This design was chosen because it:

- reduces operational complexity
- centralizes the ingestion logic for reference data
- improves maintainability
- provides a cleaner architecture for portfolio and real-world implementation

---

## File Validation Rule

The Logic App validates incoming files based on file name.

### Accepted files

- reference_calendar.csv
- reference_locations.csv
- reference_sales_channels.csv

### Validation logic

- if file name matches expected values → process normally
- if file name does not match → route to rejected area

This ensures that only known datasets are ingested into the official RAW structure.

---

## Landing Targets in RAW

Each valid file is routed to its corresponding dataset path in RAW using timestamp-based versioning:

- `reference_calendar.csv`
  -> `raw/reference/reference_calendar/load_date=YYYY-MM-DD/reference_calendar_<timestamp>.csv`

- `reference_locations.csv`
  -> `raw/reference/reference_locations/load_date=YYYY-MM-DD/reference_locations_<timestamp>.csv`

- `reference_sales_channels.csv`
  -> `raw/reference/reference_sales_channels/load_date=YYYY-MM-DD/reference_sales_channels_<timestamp>.csv`

### Timestamp format

The timestamp follows UTC standard:

yyyyMMddTHHmmssZ

Example:

reference_calendar_20260319T151315Z.csv


---

## Load Date Behavior

The `load_date` partition represents the date of ingestion into the Data Lake, not necessarily the internal date of the file content.

This preserves landing history and aligns with the platform convention already defined for RAW.

---

## RAW Behavior Rules

The RAW layer follows strict data preservation principles:

- no file overwrite is allowed
- every ingestion event generates a new file
- files are versioned using timestamp
- multiple files can exist for the same dataset within the same `load_date`
- RAW must preserve all historical arrivals for traceability and reprocessing

---

## Rejected Files Handling

Files that do not match the expected naming contract must not be ingested into the official RAW dataset paths.

### Rejected zone

Invalid files are stored in a dedicated rejected area inside the reference domain:

raw/reference/_rejected/load_date=YYYY-MM-DD/

### Behavior

When a file is rejected:

- it is not written to the official RAW dataset path
- it is stored in the rejected area with timestamp
- the original file name is preserved as the base name
- the event is logged through Logic App execution history
- the file remains available for investigation and audit

### Example

raw/reference/_rejected/load_date=2026-03-19/reference_calender_20260319T151315Z.csv


---

## Notification

Each rejected file triggers an operational notification to:

rm@rmdatasolutions.net


### Notification channel

The current implementation uses SMTP-based email sending from the Logic App.

### Notification content

The alert includes:

- rejected file name
- detection timestamp in UTC
- SharePoint source context
- rejection reason
- target rejected path in ADLS

This provides immediate operational visibility when unexpected files are placed in the SharePoint reference folder.

---

## Authentication Strategy

### SharePoint

The SharePoint connection uses a dedicated service account:

svc.sharepoint.ingestion@rmdatasolutions.net


This account was created specifically for ingestion purposes and validated with read-only access to the SharePoint site and monitored folder.

### Azure Storage

The Blob Storage connection uses the Logic App managed identity.

This design avoids hardcoded credentials and aligns with modern Azure authentication practices.

### RBAC approach

Access to storage is granted through a security group-based RBAC strategy, improving maintainability and governance of permissions.

---

## Why This Strategy Was Chosen

This ingestion strategy reflects a practical balance between realism and confidentiality.

The project is based on a real business scenario, but real production data cannot be used. As a result:

- manual ingestion is used to represent realistic enterprise sources with synthetic data
- Logic Apps is used where event-driven or business-managed ingestion adds architectural value
- ADF remains focused on orchestration rather than direct landing
- RAW preserves all arrivals without overwrite
- rejected files remain visible and auditable instead of being silently discarded

This keeps the platform aligned with real-world data engineering practices while remaining safe from a confidentiality perspective.

---

## Current Operational State

At the current stage of the project, the SharePoint reference ingestion flow is operational and validated end-to-end.

Validated capabilities include:

- detection of valid and invalid files in SharePoint
- file content retrieval from SharePoint
- whitelist-based validation of accepted file names
- landing of valid files in the correct RAW dataset paths
- landing of invalid files in `raw/reference/_rejected/`
- timestamp-based versioning
- email notification for rejected files

---

## Future Evolution

This ingestion model can evolve in future iterations to include:

- workflow export and versioning as code
- Terraform integration of Logic App workflow definition
- stronger operational logging and observability
- ingestion metadata tracking
- CI/CD integration for ingestion-related artifacts
- validation and reconciliation workflows after landing
- standardized alerting patterns across other ingestion sources

