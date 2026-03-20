# Architecture Overview — PT Frozen Foods

## Overview

This document describes the architecture of the PT Frozen Foods data platform.

The platform is designed as a Lakehouse architecture built on Microsoft Azure, combining scalable storage, distributed processing, and orchestrated data pipelines.

Although the project uses synthetic data and a fictional company name due to confidentiality constraints, the architecture, decisions, and implementation approach follow real-world enterprise standards.

---

## Architectural Principles

The platform is designed based on the following principles:

- separation of concerns between ingestion, orchestration, and processing
- scalability and modularity
- data traceability and auditability
- minimal coupling between services
- Infrastructure as Code (IaC) as foundation
- readiness for CI/CD integration
- security-first approach using identity-based access

---

## High-Level Architecture

The architecture follows a layered Lakehouse model:

Data Sources  
→ Ingestion Layer  
→ RAW (Data Lake)  
→ Processing Layer (Databricks)  
→ Bronze → Silver → Gold  
→ Analytics / Machine Learning  

---

## Core Azure Services

The platform uses the following Azure services:

### Azure Data Lake Storage Gen2 (ADLS)

- central storage layer
- stores RAW, Bronze, Silver, Gold data
- hierarchical namespace enabled
- supports large-scale data processing

### Azure Data Factory (ADF)

- orchestration layer
- triggers and coordinates data pipelines
- manages dependencies between processing steps

### Azure Databricks (DBX)

- processing engine
- performs transformations across Bronze, Silver, Gold layers
- supports scalable distributed computation
- used for both exploratory and production workloads

### Azure Logic Apps

- ingestion automation layer (for selected sources)
- integrates with SharePoint
- performs validation and routing logic
- triggers alerts for rejected data

### Azure Key Vault

- secure storage for secrets and credentials
- supports identity-based access

### Azure Monitor / Log Analytics

- monitoring and observability
- collects logs and metrics from platform components

---

## Infrastructure as Code

All infrastructure components are provisioned using Terraform.

### Benefits

- reproducibility
- version control
- environment consistency
- easier maintenance and scaling

### Managed resources

- resource group
- storage account (ADLS Gen2)
- containers (raw, bronze, silver, gold)
- data factory
- databricks workspace
- key vault
- monitoring workspace

---

## Ingestion Layer

The ingestion layer is implemented using a hybrid approach:

### Manual ingestion

Used for:

- CRM datasets
- ERP datasets
- weather data
- web logs

These datasets are manually uploaded to the RAW layer to simulate enterprise systems while preserving confidentiality.

### Automated ingestion (Logic App)

Used for:

- reference datasets

Process:

SharePoint → Logic App → ADLS RAW

Capabilities:

- trigger on file creation or modification
- file content retrieval
- validation based on file name
- routing to correct dataset path
- rejected file handling
- email alerting

---

## RAW Layer

The RAW layer is the initial landing zone for all data.

### Characteristics

- stores data as received
- no transformations applied
- immutable (no overwrite)
- timestamp-based versioning
- partitioned by load_date

### Structure

raw/<domain>/<dataset>/load_date=YYYY-MM-DD/

### Rejected zone

raw/reference/_rejected/load_date=YYYY-MM-DD/

---

## Processing Layer

The processing layer is implemented in Azure Databricks.

### Bronze

- initial transformation
- parsing and schema enforcement
- conversion to Delta format

### Silver

- data cleaning
- enrichment
- deduplication
- integration across sources

### Gold

- business-ready datasets
- aggregation and modeling
- support for analytics and ML

---

## Orchestration Layer

Azure Data Factory orchestrates processing workflows.

### Responsibilities

- trigger Databricks notebooks
- manage pipeline dependencies
- coordinate execution flow
- enable scheduling and monitoring

ADF does not perform ingestion in this phase.

---

## Security and Access Control

The platform follows identity-based security practices.

### Managed Identity

- used by Logic App for storage access
- avoids credential exposure

### Service Accounts

- SharePoint ingestion uses dedicated service account:
  svc.sharepoint.ingestion@rmdatasolutions.net

### RBAC

- access controlled via security groups
- permissions assigned at resource level

---

## Data Governance

The architecture enforces governance through:

- strict RAW layer rules (no overwrite)
- validation of incoming files
- rejected data handling
- email alerts for anomalies
- naming conventions
- partitioning standards

---

## Observability

The platform includes monitoring and logging capabilities:

- Logic App run history
- Azure Monitor integration
- Log Analytics workspace

These components enable:

- troubleshooting
- performance tracking
- operational visibility

---

## Current Implementation Status

The following components are fully implemented:

- infrastructure provisioned via Terraform
- storage structure created
- RAW layer defined and populated
- manual ingestion completed
- SharePoint ingestion via Logic App implemented
- validation and rejected handling operational
- email alerting configured
- authentication and RBAC configured

---

## Future Evolution

The architecture is designed to evolve with:

- full CI/CD integration
- Logic App workflow export and versioning
- deeper monitoring and alerting
- metadata-driven ingestion
- advanced data quality checks
- expansion of automated ingestion sources
- integration with analytics and BI tools

---

## Conclusion

The PT Frozen Foods platform architecture is aligned with enterprise-grade data engineering practices.

It provides:

- scalable and modular design
- clear separation of responsibilities
- robust ingestion layer
- strong data governance
- readiness for advanced analytics and machine learning

This foundation enables the platform to evolve into a fully production-ready data ecosystem.