# RAW Implementation Plan — PT Frozen Foods

## Objectives

This document defines how the RAW layer ingestion is implemented for the PT Frozen Foods platform.

The goal is to ensure a consistent, controlled, and traceable ingestion process aligned with enterprise data engineering practices.

---

## Scope

This plan covers:

- ingestion of reference data via SharePoint + Logic App
- storage rules in the RAW layer
- file validation and routing logic
- rejected files handling
- versioning and partitioning strategy

---

## Ingestion Flow

The ingestion process follows these steps:

1. file is created or modified in SharePoint
2. Logic App is triggered
3. file metadata is retrieved
4. file content is read
5. file name is validated
6. timestamp is generated (UTC)
7. load_date is generated (UTC)
8. file is routed to:
   - RAW dataset (valid file)
   - rejected zone (invalid file)

---

## File Validation

### Accepted files

- reference_calendar.csv
- reference_locations.csv
- reference_sales_channels.csv

### Validation behavior

- if file name matches expected list → proceed to RAW
- if file name does not match → route to rejected zone

This validation ensures that only known datasets enter the official RAW structure.

---

## Path Structures

### Valid files

raw/reference/<dataset>/load_date=YYYY-MM-DD/<dataset>_<timestamp>.csv

### Examples

raw/reference/reference_calendar/load_date=2026-03-18/reference_calendar_20260318T101500Z.csv

---

### Rejected files

raw/rejected/sharepoint_reference/load_date=YYYY-MM-DD/<filename>_<timestamp>.csv

### Example

raw/rejected/sharepoint_reference/load_date=2026-03-18/reference_calender_20260318T101500Z.csv

---

## Versioning Strategy

The RAW layer follows an append-only approach.

### Rules

- no overwrite is allowed
- every ingestion generates a new file
- multiple versions can exist within the same load_date
- versioning is controlled via timestamp in file name

---

## Timestamp Generation

### Format

yyyyMMddTHHmmssZ

### Rules

- must be generated at ingestion time
- must use UTC
- must be independent from file content

---

## Load Date Generation

### Format

load_date=YYYY-MM-DD

### Rules

- based on UTC date
- represents ingestion time
- not derived from file content

---

## Rejected Files Handling

Rejected files are files that do not comply with the expected naming contract.

### Behavior

- must not be stored in official RAW dataset paths
- must be stored in rejected zone
- must include timestamp in file name
- must follow the same partitioning logic (load_date)
- must be preserved for audit and troubleshooting

---

## Notification

Each rejected file triggers an operational notification.

### Target

rm@rmdatasolutions.net

### Notification content

- file name
- SharePoint path
- ingestion timestamp (UTC)
- rejection reason
- expected file names

---

## Error Handling

The ingestion process must handle errors in a controlled manner.

### Types of errors

- invalid file name
- file read failure
- storage write failure

### Behavior

- errors must be logged
- ingestion must not silently fail
- rejected files must be preserved when possible

---

## Responsibilities

### Logic App

- trigger ingestion
- validate file name
- generate timestamp and load_date
- route files
- send notifications

### RAW Layer

- store files
- preserve data
- maintain traceability

---

## Design Principles

- no data loss
- no overwrite
- full traceability
- separation between ingestion and transformation
- reproducibility of ingestion events
- alignment with enterprise Lakehouse architecture