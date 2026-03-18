# RAW Layer Conventions — PT Frozen Foods

## Purpose

The RAW layer is responsible for storing data in its original form, preserving all ingested data without business transformation.

It acts as the controlled landing zone of the platform and guarantees traceability, reproducibility, and data integrity.

---

## Core Principles

The RAW layer follows strict engineering principles:

- no data loss is allowed
- no overwrite is allowed
- all ingestion events must be preserved
- data must remain in original format
- ingestion must be fully traceable
- RAW must be immutable from a business logic perspective

---

## Folder Structure

All datasets must follow a standardized structure:

raw/<domain>/<dataset>/load_date=YYYY-MM-DD/

### Example

raw/reference/reference_calendar/load_date=2026-03-18/

---

## Naming Conventions

- all names must be in **snake_case**
- no spaces allowed
- all names must be in English
- dataset names must be consistent with source systems

---

## File Naming Convention

Each file must include a timestamp to ensure versioning.

Pattern:

<dataset>_<timestamp>.csv

### Example

reference_calendar_20260318T101500Z.csv

---

## Timestamp Rules

- format: yyyyMMddTHHmmssZ
- timezone: UTC
- generated at ingestion time
- must not depend on file content

---

## Partitioning

Partitioning is based on ingestion date:

load_date=YYYY-MM-DD

### Rules

- must use UTC date
- represents ingestion time, not business date
- multiple files can exist within the same partition

---

## Versioning Strategy

The RAW layer uses append-only versioning.

### Rules

- no file overwrite is allowed
- every ingestion creates a new file
- multiple versions of the same dataset can coexist
- versioning is controlled via timestamp in file name

### Example

raw/reference/reference_calendar/load_date=2026-03-18/
- reference_calendar_20260318T091500Z.csv
- reference_calendar_20260318T141000Z.csv

---

## Rejected Zone

The RAW layer includes a dedicated rejected area for invalid or unexpected files.

### Structure

raw/rejected/<source>/load_date=YYYY-MM-DD/

### Current implementation

raw/rejected/sharepoint_reference/load_date=YYYY-MM-DD/

---

## Rejected Files Rules

Rejected files must follow these rules:

- must not be written into official dataset paths
- must be preserved for audit and investigation
- must include timestamp in file name
- must follow the same partitioning logic (load_date)
- must not be processed downstream

### Example

raw/rejected/sharepoint_reference/load_date=2026-03-18/reference_calender_20260318T101500Z.csv

---

## File Integrity

The RAW layer must preserve:

- original schema
- original column names
- original data types (as received)
- original file format

No transformations are allowed in RAW.

---

## Relationship with Bronze Layer

The RAW layer feeds the Bronze layer.

### Responsibilities separation

RAW:
- ingestion
- storage
- preservation

Bronze:
- parsing
- schema normalization
- technical enrichment

---

## Design Rationale

This design ensures:

- full data traceability
- safe reprocessing capability
- auditability of ingestion events
- separation between ingestion and transformation
- alignment with enterprise Lakehouse best practices