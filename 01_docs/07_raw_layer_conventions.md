# RAW Layer Conventions — PT Frozen Foods

## Purpose

The RAW layer stores data in its original form, preserving all ingested data without transformation.

It acts as the controlled landing zone of the platform and guarantees traceability, reproducibility, and data integrity.

---

## Core Principles

The RAW layer follows strict rules:

- no data loss is allowed
- no overwrite is allowed
- all ingestion events must be preserved
- data must remain in original format
- ingestion must be fully traceable
- RAW must be immutable from a business perspective

---

## Folder Structure

All datasets must follow:

raw/<domain>/<dataset>/load_date=YYYY-MM-DD/

### Example

raw/reference/reference_calendar/load_date=2026-03-18/

---

## Naming Conventions

- snake_case only
- no spaces or special characters
- names must be in English
- dataset names must match source systems

---

## File Naming

Each file must include a timestamp.

### Pattern

<dataset>_<timestamp>.csv

### Example

reference_calendar_20260318T101500Z.csv

---

## Timestamp Rules

- format: yyyyMMddTHHmmssZ
- timezone: UTC
- generated at ingestion time
- independent from file content

---

## Partitioning

Partitioning is based on ingestion date:

load_date=YYYY-MM-DD

### Rules

- uses UTC date
- represents ingestion time
- multiple files allowed per partition

---

## Versioning

RAW follows an append-only model.

### Rules

- no overwrite
- each ingestion creates a new file
- multiple versions can coexist
- versioning controlled by timestamp

---

## Rejected Zone

Invalid files must be stored in a rejected area within the same domain.

### Structure

raw/<domain>/_rejected/load_date=YYYY-MM-DD/

### Example

raw/reference/_rejected/load_date=2026-03-18/

---

## Rejected Files Rules

- must not be written to dataset paths
- must be preserved for audit
- must include timestamp
- must follow partitioning rules
- must not be processed downstream

---

## File Integrity

The RAW layer must preserve:

- original schema
- original column names
- original data types
- original file format

No transformations are allowed.

---

## Relationship with Bronze

RAW feeds the Bronze layer.

### Responsibility split

RAW:
- ingestion
- storage
- preservation

Bronze:
- parsing
- schema handling
- technical enrichment

---

## Design Rationale

This design ensures:

- full traceability
- safe reprocessing
- auditability
- separation of concerns
- alignment with enterprise Lakehouse standards