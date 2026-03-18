# Decisions Log — PT Frozen Foods

## Overview

This document records key architectural and technical decisions made throughout the development of the PT Frozen Foods data platform.

The goal is to ensure traceability, clarity of reasoning, and alignment with enterprise-grade data engineering practices.

---

## Ingestion Architecture

### Decision

Adopt a hybrid ingestion model combining manual ingestion and automated ingestion.

### Description

- manual ingestion is used for CRM, ERP, Web and Weather datasets
- automated ingestion is implemented using SharePoint + Azure Logic Apps for reference datasets

### Rationale

- synthetic data and confidentiality constraints prevent direct system integration
- manual ingestion preserves control and reproducibility
- automated ingestion is applied where it adds architectural value

---

## Logic App Design

### Decision

Use Azure Logic App Consumption for automated ingestion.

### Description

- single Logic App for all reference datasets
- event-driven ingestion
- trigger based on file creation or modification
- routing based on file name

### Rationale

- reduces operational complexity
- centralizes ingestion logic
- improves maintainability
- aligns with real-world architecture

---

## SharePoint Integration

### Decision

Use SharePoint as the source for reference datasets.

### Description

- monitored folder: Documents/reference
- files are business-managed reference data
- ingestion triggered by file events

### Rationale

- SharePoint is a realistic enterprise source for reference data
- allows business users to manage inputs
- supports event-driven ingestion pattern

---

## Authentication Strategy

### Decision

Use a dedicated service account for SharePoint integration.

### Service Account

svc.sharepoint.ingestion@rmdatasolutions.net

### Description

- account configured with Authenticator
- read-only access to SharePoint site and folder
- used exclusively for integration

### Rationale

- avoids dependency on personal accounts
- ensures auditability
- provides stability and control
- aligns with enterprise security practices

---

## RAW Layer Strategy

### Decision

Adopt a strict append-only RAW layer.

### Description

- no overwrite allowed
- every ingestion generates a new file
- data is preserved in original format
- ingestion is fully traceable

### Rationale

- ensures data integrity
- supports reprocessing
- enables auditability
- aligns with Lakehouse best practices

---

## Timestamp Standard

### Decision

Use UTC timestamp for file versioning.

### Format

yyyyMMddTHHmmssZ

### Description

- generated at ingestion time
- independent of data content
- used in file naming

### Rationale

- ensures consistency across systems
- avoids timezone ambiguity
- supports global scalability

---

## Partitioning Strategy

### Decision

Partition RAW data using ingestion date.

### Format

load_date=YYYY-MM-DD

### Description

- based on UTC
- represents ingestion time
- not tied to business data

### Rationale

- standardizes ingestion tracking
- simplifies downstream processing
- supports efficient data organization

---

## Rejected Files Strategy

### Decision

Introduce a dedicated rejected zone in the RAW layer.

### Structure

raw/rejected/sharepoint_reference/

### Description

- invalid files are not ingested into official datasets
- files are preserved for investigation
- files follow same partitioning and timestamp rules

### Example

raw/rejected/sharepoint_reference/load_date=2026-03-18/reference_calender_20260318T101500Z.csv

### Rationale

- prevents data loss
- avoids silent ingestion errors
- enables troubleshooting and audit

---

## File Validation Strategy

### Decision

Validate files based on file name before ingestion.

### Accepted files

- reference_calendar.csv
- reference_locations.csv
- reference_sales_channels.csv

### Behavior

- valid files are processed normally
- invalid files are routed to rejected zone

### Rationale

- ensures ingestion consistency
- protects RAW dataset integrity
- enforces source contract

---

## Notification Strategy

### Decision

Send notification for rejected files.

### Target

rm@rmdatasolutions.net

### Description

Each rejected file triggers a notification containing:

- file name
- SharePoint location
- ingestion timestamp (UTC)
- rejection reason
- expected file names

### Rationale

- ensures operational visibility
- enables quick investigation
- prevents unnoticed ingestion failures

---

## Role of ADF

### Decision

Use Azure Data Factory as orchestration layer only.

### Description

- does not perform ingestion in this project
- triggers Databricks notebooks
- coordinates Bronze, Silver and Gold processing

### Rationale

- keeps ingestion decoupled from orchestration
- simplifies architecture
- aligns with Lakehouse pattern

---

## Role of Databricks

### Decision

Use Databricks as the main processing engine.

### Description

- performs transformations
- processes Bronze, Silver and Gold layers
- prepares data for analytics and ML

### Rationale

- scalable processing engine
- industry standard for data platforms
- aligns with project goals

---

## Design Principles

The following principles guide all decisions:

- separation of concerns
- data traceability
- auditability
- controlled ingestion
- enterprise-grade architecture
- reproducibility