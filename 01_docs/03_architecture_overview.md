# Architecture Overview — PT Frozen Foods

## Overview

This document describes the architecture of the PT Frozen Foods data platform.

The platform follows a Lakehouse architecture built on Microsoft Azure, combining scalable storage, distributed processing, and governed data access.

Although the project uses synthetic data, the architecture and implementation follow real-world enterprise standards.

---

## Architectural Principles

The platform is designed based on:

- separation of concerns (ingestion, processing, orchestration, governance)
- scalability and modularity
- data traceability and auditability
- minimal coupling between services
- Infrastructure as Code (IaC)
- readiness for CI/CD
- security-first approach using identity-based access

---

## High-Level Architecture

Data Sources  
→ Ingestion Layer  
→ RAW (ADLS)  
→ Processing (Databricks)  
→ Bronze → Silver (Curated & Integration) → Gold  
→ Analytics / Machine Learning  

Governance Layer (Unity Catalog) spans across all processed data.

---

## Core Azure Services

### Azure Data Lake Storage Gen2 (ADLS)

- central storage layer
- stores RAW, Bronze, Silver, and Gold data
- hierarchical namespace enabled
- data remains physically in ADLS

---

### Azure Databricks (DBX)

- processing engine
- executes transformations across all layers
- supports Auto Loader for ingestion
- integrates with Unity Catalog for governance
- enables Delta Lake optimizations and Liquid Clustering

---

### Unity Catalog (UC)

- centralized governance layer
- manages catalogs, schemas, tables, and permissions
- controls access to data stored in ADLS
- separates storage from access control

---

### Azure Data Factory (ADF)

- orchestration layer
- triggers Databricks notebooks
- manages execution flow and dependencies

---

### Azure Logic Apps

- ingestion automation for reference data
- integrates with SharePoint
- validates and routes files to RAW
- handles rejected files and notifications

---

### Azure Key Vault

- secure storage for secrets (when needed)
- supports identity-based access

---

### Azure Monitor / Log Analytics

- monitoring and observability
- collects logs and execution metrics

---

## Infrastructure as Code

Infrastructure is provisioned using Terraform.

### Managed Resources

- resource group
- ADLS storage account
- containers (raw, bronze, silver, gold)
- Databricks workspace
- Data Factory
- Logic App
- Key Vault
- monitoring workspace
- Databricks Access Connector

---

## Ingestion Layer

The ingestion layer uses a hybrid model.

### Manual Ingestion

Used for:

- CRM
- ERP
- weather data
- web logs

Files are uploaded directly to RAW.

---

### Automated Ingestion (Logic App)

Used for:

- reference datasets

Flow:

SharePoint → Logic App → ADLS RAW

Capabilities:

- file detection
- validation
- routing
- rejected file handling
- email notification

---

## RAW Layer

The RAW layer is the landing zone.

### Characteristics

- data stored as received
- no transformation
- append-only
- timestamp-based versioning
- partitioned by `load_date`

### Structure

raw/<domain>/<dataset>/load_date=YYYY-MM-DD/

---

## Processing Layer (Databricks)

Processing is performed in Azure Databricks following the Medallion Architecture.

### Bronze Layer

- ingestion from RAW using Auto Loader
- incremental processing
- schema evolution enabled
- minimal transformations
- data stored in Delta format
- data written to explicit ADLS paths
- tables registered in Unity Catalog

---

### Silver Layer

- data cleaning and validation
- business-aware transformations
- joins and enrichment
- reusable datasets
- integration of curated datasets across domains (e.g., ERP and CRM)
- preparation of analytics-ready datasets for the Gold layer

This layer includes both **Curated** and **Integration** datasets.

---

### Gold Layer

- analytical datasets
- aggregations and business metrics
- dimensional models and data marts
- BI and machine learning-ready outputs

---

### Optimization Standards

To ensure performance and scalability, the platform adopts modern Lakehouse optimizations:

- Delta Lake as the storage format
- external tables stored in ADLS Gen2
- Liquid Clustering for optimized data layout
- Auto Optimize for efficient writes and compaction
- Photon engine for accelerated query performance
- Adaptive Query Execution (AQE)
- column pruning and optimized join strategies

---

## Governance and Data Access

Data access is controlled by Unity Catalog.

### Storage Access

- Databricks accesses ADLS via Access Connector
- Managed Identity is used (no secrets)

---

### External Locations

- define access to ADLS paths
- linked to storage credentials
- used for RAW, Bronze, and Silver access

---

### Permissions

- managed via Unity Catalog (GRANTS)
- applied to users or groups
- examples:
  - READ FILES
  - WRITE FILES
  - SELECT
  - MODIFY

---

## Orchestration Layer

Azure Data Factory (ADF) is responsible for orchestration.

### Responsibilities

- trigger Databricks notebooks
- manage execution order
- schedule pipelines

ADF does not perform ingestion or transformation.

---

## Security Model

The platform uses identity-based access.

### Managed Identity

- used for storage access
- avoids credentials

---

### RBAC

- applied at storage level
- managed via Azure roles

---

### Service Accounts

- used for SharePoint ingestion
- improves governance and auditability

---

## Observability

Monitoring includes:

- Logic App execution logs
- Databricks job monitoring
- Azure Monitor and Log Analytics

---

## Current Implementation Status

Implemented:

- infrastructure via Terraform
- ADLS structure (RAW, Bronze, Silver, Gold)
- Databricks workspace (Premium)
- Unity Catalog configured
- Access Connector and RBAC configured
- External Locations and permissions defined
- ingestion via Logic App operational
- RAW layer populated
- Bronze ingestion with Auto Loader implemented
- Silver layer transformations implemented
- Silver integration datasets created and optimized using Delta Lake and Liquid Clustering

---

## Future Evolution

Planned improvements:

- CI/CD integration
- Terraform expansion for governance
- monitoring enhancements
- data quality framework
- metadata-driven pipelines
- BI and ML integration

---

## Conclusion

The PT Frozen Foods platform follows enterprise data architecture standards.

It provides:

- scalable storage and processing
- strong governance with Unity Catalog
- clear separation of responsibilities
- secure and auditable data access
- readiness for advanced analytics

This foundation supports the evolution into a fully production-ready data platform.