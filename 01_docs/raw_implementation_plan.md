# RAW Implementation Plan — PT Frozen Foods

## Overview

This document defines how the RAW layer is physically implemented in the PT Frozen Foods data platform.

The RAW layer represents the initial landing zone of all incoming data and must preserve the original structure and content of source files. It is a critical component of the Lakehouse architecture and must follow strict rules to ensure traceability, reproducibility, and auditability.

This document reflects the current implementation state, including both manual ingestion and automated ingestion via Azure Logic Apps integrated with SharePoint.

---

## RAW Layer Objectives

The RAW layer is designed to:

- store data exactly as received from source systems
- preserve full historical arrival of files
- isolate ingestion from transformation logic
- support reprocessing scenarios
- enable traceability and auditability
- prevent data loss during ingestion

---

## Storage Structure

The RAW layer is implemented in Azure Data Lake Storage Gen2 (ADLS Gen2) using the following hierarchical structure:

domain → dataset → partition

### Domains

The following domains are defined:

- crm
- erp
- reference
- weather_api
- web

### Dataset structure

Each dataset is stored under its respective domain:

raw/<domain>/<dataset>/load_date=YYYY-MM-DD/

Example:

raw/crm/crm_clients/load_date=2026-03-19/

---

## Naming Conventions

The RAW layer follows strict naming conventions:

- all names in lowercase
- snake_case format
- no spaces or special characters
- dataset names aligned with business entities

---

## Load Date Partition

Each dataset is partitioned by ingestion date using:

load_date=YYYY-MM-DD

Important:

- load_date represents ingestion time, not business event time
- multiple files may exist within the same partition
- partitioning supports scalable processing and filtering

---

## File Versioning Strategy

To prevent overwriting and ensure full historical traceability, all files are versioned using a UTC timestamp.

### File naming pattern

<dataset>_yyyyMMddTHHmmssZ.csv

Example:

reference_calendar_20260319T151315Z.csv

### Rules

- no file overwrite is allowed
- every ingestion generates a new file
- multiple versions of the same dataset can exist within the same load_date
- timestamps ensure uniqueness and ordering of arrivals

---

## Manual Ingestion Implementation

Manual ingestion is used for synthetic datasets representing enterprise systems.

### Sources

- CRM datasets
- ERP datasets
- weather data
- web event logs

### Process

- files are manually uploaded to ADLS
- files are placed directly into the correct dataset folder
- users must follow naming and partition conventions

---

## SharePoint + Logic App Ingestion

Reference datasets are ingested automatically using Azure Logic Apps integrated with SharePoint.

### SharePoint configuration

- tenant: rmdatascience.sharepoint.com
- site: /sites/PT-Frozen-Foods-Data
- document library: Documents
- monitored folder: reference

### Files monitored

- reference_calendar.csv
- reference_locations.csv
- reference_sales_channels.csv

---

## Logic App Workflow

The ingestion workflow consists of:

1. trigger on file creation or modification
2. retrieve file content from SharePoint
3. validate file name
4. route file based on validation result
5. store file in ADLS
6. send notification if rejected

---

## Validation Logic

The Logic App enforces a whitelist validation rule.

### Accepted files

- reference_calendar.csv
- reference_locations.csv
- reference_sales_channels.csv

### Behavior

- valid files → processed and stored in RAW
- invalid files → redirected to rejected area

---

## Valid File Handling

Valid files are stored in the RAW layer using dataset-based routing.

### Target structure

raw/reference/<dataset>/load_date=YYYY-MM-DD/<dataset>_<timestamp>.csv

Example:

raw/reference/reference_calendar/load_date=2026-03-19/reference_calendar_20260319T151315Z.csv

---

## Rejected File Handling

Invalid files are not discarded. Instead, they are stored for audit and investigation.

### Rejected path

raw/reference/_rejected/load_date=YYYY-MM-DD/

### Behavior

- rejected files are stored with timestamp
- original filename is preserved as base
- no overwrite occurs
- files remain available for analysis

### Example

raw/reference/_rejected/load_date=2026-03-19/invalid_file_20260319T151315Z.csv

---

## Notification Mechanism

Rejected files trigger an automatic email notification.

### Recipient

rm@rmdatasolutions.net

### Channel

SMTP (Gmail App Password)

### Notification content

- file name
- ingestion timestamp (UTC)
- rejection reason
- target storage location

This ensures operational visibility and fast issue detection.

---

## Authentication Strategy

### SharePoint

- dedicated service account used:
  svc.sharepoint.ingestion@rmdatasolutions.net
- read access to monitored folder

### Azure Storage

- Logic App uses Managed Identity
- avoids credential exposure

### RBAC

- access managed via security groups
- improves governance and scalability

---

## RAW Layer Rules (Strict)

The following rules must always be respected:

- no transformations allowed
- no overwriting of files
- all files must be versioned
- all ingestion events must be preserved
- rejected files must not be deleted automatically
- ingestion must not fail silently

---

## Current Implementation Status

The RAW layer is fully implemented and validated.

### Completed

- storage structure created
- domains and datasets defined
- manual ingestion operational
- SharePoint ingestion operational
- Logic App workflow implemented
- validation rules active
- rejected handling implemented
- email alert implemented
- timestamp versioning validated

---

## Next Steps

The next phase of the project will focus on:

- Bronze layer implementation in Databricks
- integration with ADF orchestration
- workflow export to JSON
- Terraform integration for Logic App
- CI/CD enablement
- enhanced monitoring and logging

---

## Conclusion

The RAW layer implementation is complete and aligned with enterprise-grade data engineering practices.

It ensures:

- full data traceability
- ingestion reliability
- controlled data entry
- operational visibility
- readiness for scalable downstream processing

This provides a strong and robust foundation for the next stages of the Lakehouse architecture.