# Architecture Overview — PT Frozen Foods

## Overview

The PT Frozen Foods platform is designed following a **Lakehouse architecture** pattern implemented on Microsoft Azure.

The architecture separates ingestion, storage, processing, and consumption into clearly defined layers, ensuring scalability, maintainability, and alignment with enterprise data engineering practices.

---

## Architecture Principles

The platform follows these core principles:

- separation of concerns between ingestion, orchestration, and processing
- centralized storage in a Data Lake
- schema evolution handled in processing layers
- reproducible and traceable data pipelines
- Infrastructure as Code (IaC) for provisioning
- modular and extensible design

---

## High-Level Architecture

The platform is composed of the following components:

### Data Sources

- CRM (synthetic datasets)
- ERP (synthetic datasets)
- Weather API (synthetic dataset)
- Web logs (synthetic dataset)
- SharePoint (reference data)

---

### Ingestion Layer

The ingestion layer is responsible for landing data into the RAW layer.

It follows a hybrid model:

#### Manual ingestion

- used for CRM, ERP, Web, and Weather datasets
- represents real enterprise systems under confidentiality constraints

#### Automated ingestion

- implemented using SharePoint + Azure Logic Apps
- used for reference datasets
- event-driven ingestion based on file creation or modification

---

## Automated Ingestion — SharePoint

Reference data ingestion is implemented using SharePoint integrated with Azure Logic Apps.

### Flow

SharePoint → Logic App → ADLS (RAW)

### Source Configuration

- tenant host: `rmdatascience.sharepoint.com`
- site path: `/sites/PT-Frozen-Foods-Data`
- document library: `Documents`
- monitored folder: `reference`

### Trigger Behavior

The Logic App is triggered when a file is created or modified in the monitored folder.

This ensures that both new files and updates to existing files are captured automatically.

### Supported Files

- reference_calendar.csv
- reference_locations.csv
- reference_sales_channels.csv

### Routing Logic

Files are routed based on file name to their corresponding RAW dataset paths.

### RAW Storage Rules

- files are stored using timestamp-based versioning
- no overwrite is allowed
- multiple versions can exist within the same load_date partition
- ingestion date is based on UTC

### Rejected Files Handling

Files that do not match the expected naming contract are routed to a rejected zone.

#### Structure

raw/rejected/sharepoint_reference/load_date=YYYY-MM-DD/

#### Behavior

- files are preserved for audit and investigation
- files are not processed downstream
- ingestion events are logged
- notification is sent to operational stakeholders

### Notification

Rejected files trigger an email notification to:

rm@rmdatasolutions.net

### Authentication

- SharePoint access is performed using a dedicated service account
- service account: svc.sharepoint.ingestion@rmdatasolutions.net
- storage access is designed to use managed identity (planned)

### Design Characteristics

- event-driven ingestion
- centralized ingestion logic
- clear separation between ingestion and processing
- high traceability and observability
- aligned with enterprise data platform patterns

---

## Storage Layer (ADLS Gen2)

Azure Data Lake Storage Gen2 is the central storage of the platform.

It is organized into the following layers:

- RAW
- Bronze
- Silver
- Gold

### RAW Layer

- stores data in original format
- no transformations applied
- append-only (no overwrite)
- partitioned by ingestion date (UTC)
- supports full traceability

---

## Processing Layer

### Azure Databricks

Databricks is the primary processing engine.

Responsibilities:

- data exploration
- parsing and schema definition
- transformation (Bronze → Silver → Gold)
- data enrichment and integration
- preparation for analytics and ML

---

## Orchestration Layer

### Azure Data Factory (ADF)

ADF is used strictly as an orchestration layer.

Responsibilities:

- trigger Databricks notebooks
- manage pipeline dependencies
- coordinate processing flows

ADF does not perform ingestion in this project.

---

## Monitoring and Observability

Monitoring is implemented using:

- Azure Monitor
- Log Analytics

Key capabilities:

- pipeline monitoring
- execution logs
- error tracking
- ingestion traceability

---

## Security and Access

### SharePoint

- accessed via dedicated service account
- read-only permissions
- controlled and auditable access

### Azure Storage

- access designed to use managed identity
- avoids hardcoded credentials
- aligned with modern security practices

---

## Infrastructure as Code

The platform infrastructure is provisioned using Terraform.

Provisioned resources include:

- Resource Group
- ADLS Gen2
- Azure Data Factory
- Azure Databricks
- Key Vault
- Log Analytics
- (planned) Logic App

---

## Future Evolution

The architecture is designed to evolve into a fully automated enterprise platform.

Potential future improvements:

- expanded automated ingestion
- event-driven architecture enhancements
- metadata-driven pipelines
- CI/CD integration
- data quality and validation frameworks
- advanced monitoring and alerting