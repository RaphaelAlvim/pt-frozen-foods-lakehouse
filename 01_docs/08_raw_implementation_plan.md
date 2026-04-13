# RAW Implementation Plan — PT Frozen Foods

## Overview

This document defines how the RAW layer is physically implemented in the PT Frozen Foods data platform.

The RAW layer is the landing zone of all incoming data and is responsible for preserving the original structure and content of source files. It is implemented in Azure Data Lake Storage Gen2 (ADLS Gen2).

It is designed to ensure traceability, reproducibility, and auditability.

---

## Objectives

The RAW layer must:

- store data exactly as received
- preserve the full history of arrivals
- isolate ingestion from processing
- support reprocessing scenarios
- prevent data loss

---

## Storage Structure

The RAW layer is implemented in ADLS Gen2 using a hierarchical structure:

domain → dataset → partition

### Domains

- crm
- erp
- reference
- weather_api
- web

These domains align with the ingestion strategy and enterprise data classification.

---

## Dataset Structure

Each dataset follows:

raw/<domain>/<dataset>/load_date=YYYY-MM-DD/

### Example

raw/crm/crm_clients/load_date=2026-03-19/

---

## Naming Conventions

- lowercase only
- snake_case
- no spaces or special characters
- dataset names aligned with business entities
- naming aligned with source systems

---

## Load Date Partition

Data is partitioned using:

load_date=YYYY-MM-DD

### Rules

- represents ingestion date
- not related to business event time
- uses UTC timezone
- multiple files allowed per partition

---

## File Versioning

All files are versioned using UTC timestamps.

### Pattern

<dataset>_yyyyMMddTHHmmssZ.csv

### Example

reference_calendar_20260319T151315Z.csv

### Rules

- no overwrite allowed
- each ingestion creates a new file
- multiple versions per day supported
- ensures traceability and auditability

---

## RAW Layer Rules

The following rules are mandatory:

- no transformation allowed
- append-only storage
- no overwrite
- all ingestion events preserved
- files must follow naming conventions
- data must remain in its original format

---

## Relationship with Processing

The RAW layer is consumed by Azure Databricks for Bronze ingestion.

- data is read incrementally from RAW
- no direct transformation occurs in RAW
- RAW serves as the single source of truth for ingestion
- Auto Loader enables scalable and incremental processing

---

## Current State

The RAW layer is fully implemented and operational.

- storage structure created in ADLS Gen2
- datasets organized by domain
- naming and partition rules applied
- versioning strategy in place
- ingestion workflows validated
- ready for Bronze processing via Databricks

---

## Conclusion

The RAW implementation ensures a reliable and auditable data foundation.

It provides:

- full traceability
- ingestion consistency
- support for incremental processing
- alignment with Lakehouse architecture principles
- readiness for enterprise-grade data governance