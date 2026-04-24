# Decisions Log — PT Frozen Foods

## Overview

This document records key architectural, technical, and operational decisions made during the development of the PT Frozen Foods data platform.

The purpose of this log is to:

- provide traceability of decisions
- document reasoning behind architectural choices
- support future evolution of the platform
- ensure alignment with enterprise-grade practices

All decisions recorded here are considered deliberate and aligned with real-world data engineering standards.

---

## Decision 001 — Use of Lakehouse Architecture

### Decision

Adopt a Lakehouse architecture based on Azure services.

### Rationale

- combines flexibility of Data Lake with structure of Data Warehouse
- supports scalable analytics and machine learning
- aligns with modern data platform standards

### Impact

- data organized into RAW, Bronze, Silver, Gold layers
- separation between ingestion, processing, and consumption

---

## Decision 002 — Use of Synthetic Data

### Decision

Use synthetic datasets instead of real production data.

### Rationale

- confidentiality constraints
- ability to simulate realistic enterprise scenarios
- full control over data structure

### Impact

- manual ingestion required for some sources
- architecture remains production-oriented

---

## Decision 003 — Hybrid Ingestion Strategy

### Decision

Adopt a hybrid ingestion approach combining manual upload and automated ingestion.

### Rationale

- manual ingestion allows controlled simulation of enterprise systems
- automated ingestion adds realism where appropriate

### Impact

- CRM, ERP, Web, Weather → manual ingestion
- Reference data → SharePoint + Logic App

---

## Decision 004 — ADF as Orchestration Layer

### Decision

Use Azure Data Factory primarily as an orchestration engine.

### Rationale

- separates ingestion from orchestration
- aligns with modern architecture patterns
- simplifies pipeline management

### Impact

- ADF triggers Databricks notebooks
- ADF does not perform direct ingestion in this phase

---

## Decision 005 — Databricks for Processing

### Decision

Use Azure Databricks for all data transformation.

### Rationale

- scalable processing engine
- supports structured and unstructured data
- integrates with Delta Lake

### Impact

- Bronze, Silver, Gold layers implemented in Databricks
- notebooks used for both exploration and production pipelines

---

## Decision 006 — RAW Layer as Immutable Storage

### Decision

RAW layer must preserve all data without overwrite.

### Rationale

- ensures traceability
- supports reprocessing
- prevents data loss

### Impact

- timestamp-based file versioning implemented
- multiple files allowed per partition

---

## Decision 007 — Timestamp-Based File Versioning

### Decision

Use UTC timestamp in file names to ensure uniqueness.

### Rationale

- avoids overwriting files
- preserves multiple ingestion events per day
- enables auditability

### Impact

- file naming pattern:
  <dataset>_yyyyMMddTHHmmssZ.csv

---

## Decision 008 — Use of load_date Partition

### Decision

Partition RAW data using load_date.

### Rationale

- aligns ingestion with partitioning strategy
- improves performance in downstream processing
- separates ingestion time from business time

### Impact

- structure:
  load_date=YYYY-MM-DD

---

## Decision 009 — Single Logic App for Reference Ingestion

### Decision

Use a single Logic App to handle all reference files.

### Rationale

- reduces complexity
- centralizes ingestion logic
- improves maintainability

### Impact

- routing logic implemented inside workflow
- validation based on file name

---

## Decision 010 — File Name-Based Validation

### Decision

Validate incoming files using file name whitelist.

### Rationale

- ensures only expected datasets are processed
- avoids ingestion of incorrect files
- simple and effective control mechanism

### Impact

- accepted:
  reference_calendar.csv
  reference_locations.csv
  reference_sales_channels.csv
- others → rejected

---

## Decision 011 — Rejected Files Must Be Preserved

### Decision

Rejected files must not be discarded.

### Rationale

- prevents silent data loss
- supports debugging and investigation
- improves governance

### Impact

- rejected files stored in:
  raw/reference/_rejected/load_date=YYYY-MM-DD/

---

## Decision 012 — Rejected Files Inside Domain

### Decision

Store rejected files within the reference domain.

### Rationale

- maintains context of origin
- simplifies investigation
- avoids centralized clutter

### Impact

- structure:
  raw/reference/_rejected/

---

## Decision 013 — Email Notification for Rejected Files

### Decision

Send email alert when a file is rejected.

### Rationale

- ensures operational visibility
- enables quick response to issues
- prevents unnoticed ingestion failures

### Impact

- email sent to:
  rm@rmdatasolutions.net
- triggered by Logic App

---

## Decision 014 — Use of SMTP for Email Alerts

### Decision

Use SMTP with Gmail App Password for email notifications.

### Rationale

- avoids dependency on Outlook connector
- works with existing Gmail account
- simple and effective implementation

### Impact

- Logic App uses SMTP connector
- authentication via App Password

---

## Decision 015 — Managed Identity for Storage Access

### Decision

Use Managed Identity for Logic App access to ADLS.

### Rationale

- avoids hardcoded credentials
- improves security
- aligns with Azure best practices

### Impact

- RBAC permissions required on storage account

---

## Decision 016 — Service Account for SharePoint

### Decision

Use dedicated service account for SharePoint access.

### Rationale

- avoids personal account usage
- improves governance and auditability
- supports production-grade design

### Impact

- account:
  svc.sharepoint.ingestion@rmdatasolutions.net

---

## Decision 017 — Group-Based RBAC

### Decision

Use security groups to manage access permissions.

### Rationale

- simplifies permission management
- improves scalability
- aligns with enterprise governance

### Impact

- roles assigned to groups instead of individuals

---

## Decision 018 — Polling-Based Trigger Strategy

### Decision

Use polling trigger for SharePoint ingestion.

### Rationale

- SharePoint connector uses polling
- acceptable due to low data volume

### Impact

- frequency configurable
- cost controlled via interval tuning

---

## Decision 019 — No Overwrite in RAW

### Decision

Do not overwrite files even within the same day.

### Rationale

- preserves full ingestion history
- avoids accidental data loss

### Impact

- timestamp mandatory in file name
- multiple files per partition allowed

---

## Decision 020 — Implementation First, Code Later

### Decision

Build Logic App manually before converting to code.

### Rationale

- faster iteration
- easier debugging
- reduces complexity during design phase

### Impact

- workflow validated via UI
- future export to JSON planned

---

## Decision 021 — Email Alert After Storage

### Decision

Send email only after rejected file is successfully stored.

### Rationale

- ensures traceability before notification
- avoids alerts without persisted data

### Impact

- email step placed after rejected blob creation

---

## Decision 022 — Use of UTC Time Standard

### Decision

Standardize all timestamps in UTC.

### Rationale

- avoids timezone inconsistencies
- aligns with global data practices

### Impact

- used in:
  - file naming
  - email notifications
  - ingestion tracking

---

## Decision 023 — Logic App Workflow Versioning Strategy

### Decision

Store Logic App workflow as versioned JSON in the repository without immediate Terraform integration.

### Rationale

- prioritize functional validation before IaC integration
- reduce complexity related to connectors and authentication
- align with real-world practices where workflows are stabilized before full automation

### Impact

- workflow is versioned and documented
- future integration with Terraform remains possible

---

## Decision 024 — Upgrade Databricks to Premium Tier

### Decision

Upgrade Azure Databricks workspace from Standard to Premium.

### Rationale

- required to enable Unity Catalog
- enables modern access modes and governance features
- aligns with enterprise-grade architecture

### Impact

- workspace upgraded without recreation
- new features available for data governance and security

---

## Decision 025 — Adoption of Unity Catalog as Governance Layer

### Decision

Adopt Unity Catalog as the central governance layer for all data assets.

### Rationale

- centralized control of data access
- improved security and auditability
- standard for modern Databricks environments

### Impact

- creation of catalog and schemas (bronze, silver, gold)
- tables registered under Unity Catalog
- separation between storage and governance

---

## Decision 026 — Use of Managed Identity with Access Connector for Databricks

### Decision

Use Access Connector with Managed Identity for Databricks access to ADLS.

### Rationale

- eliminates need for secrets and credentials
- improves security and maintainability
- aligns with Azure best practices

### Impact

- RBAC permissions assigned to Managed Identity
- Databricks accesses storage without secrets

---

## Decision 027 — Use of Storage Credential and External Locations

### Decision

Use Unity Catalog Storage Credentials and External Locations to access ADLS data.

### Rationale

- decouples storage access from compute logic
- centralizes permission management
- enables secure and scalable data access

### Impact

- creation of storage credential using Managed Identity
- creation of external locations for RAW and Bronze
- data access controlled via UC permissions

---

## Decision 028 — Use of Explicit Paths for Bronze Layer Storage

### Decision

Store Bronze datasets using explicit paths in ADLS instead of relying on managed table locations.

### Rationale

- improves visibility of data in storage
- simplifies debugging and inspection
- aligns with data lake organization principles

### Impact

- datasets stored in structured paths:
  bronze/<domain>/<dataset>/
- tables registered in UC using LOCATION

---

## Decision 029 — Use of Auto Loader for RAW to Bronze Ingestion

### Decision

Use Databricks Auto Loader for incremental ingestion from RAW to Bronze.

### Rationale

- supports scalable file ingestion
- automatically detects new files
- supports schema evolution

### Impact

- streaming ingestion pattern adopted
- use of checkpoint and schema tracking locations
- ingestion runs in batch mode using availableNow

---

## Decision 030 — Schema Evolution Enabled in Bronze Layer

### Decision

Allow schema evolution during RAW to Bronze ingestion.

### Rationale

- supports changes in source data structure
- reduces ingestion failures
- improves pipeline robustness

### Impact

- new columns automatically added
- schema tracked in dedicated schema location

---

## Conclusion

The decisions recorded in this document reflect a consistent effort to align the PT Frozen Foods platform with real-world enterprise data engineering practices.

These decisions ensure:

- data reliability
- operational visibility
- scalability
- maintainability
- governance and security

This log should be continuously updated as the platform evolves.

---

## Decision 031 — Adoption of External Tables Across the Lakehouse

### Decision
Adopt external Delta tables stored in Azure Data Lake Storage Gen2 across Bronze, Silver, and Gold layers.

### Rationale
- ensures separation between storage and compute
- improves interoperability with Azure services
- aligns with modern Lakehouse architecture
- enhances data visibility and governance

### Impact
- data stored directly in ADLS Gen2
- tables registered in Unity Catalog using explicit LOCATION paths
- improved portability and maintainability

---

## Decision 032 — Implementation of the Silver Integration Layer

### Decision
Introduce a dedicated Silver Integration layer to consolidate curated datasets from multiple domains.

### Rationale
- enables cross-domain analytics
- prepares integrated datasets for the Gold layer
- improves data consistency and reusability
- aligns with enterprise Medallion Architecture practices

### Impact
- integration of ERP and CRM datasets
- creation of the `silver_orders_customers` dataset
- standardized integration logic within the Silver layer

---

## Decision 033 — Deduplication Strategy for ERP Orders

### Decision
Implement deterministic deduplication for ERP order headers based on `pedido_id`.

### Rationale
- ensures a single record per order
- resolves duplicates caused by multiple updates from source systems
- preserves the correct dataset grain

### Impact
- duplicates removed using the latest ingestion timestamp
- improved data quality and consistency
- accurate downstream analytics and reporting

---

## Decision 034 — Adoption of Liquid Clustering for Performance Optimization

### Decision
Adopt Liquid Clustering for optimizing Delta tables in the Silver and Gold layers.

### Rationale
- improves query performance
- eliminates rigid partitioning strategies
- adapts dynamically to data access patterns
- aligns with modern Databricks best practices

### Impact
- clustering applied to high-value analytical datasets
- initial clustering keys:
  - data_pedido
  - cliente_id
- enhanced performance for BI and analytical workloads

---

## Decision 035 — Adoption of Auto Optimize for Delta Tables

### Decision
Enable Auto Optimize for Delta tables in the processing layers.

### Rationale
- improves write performance
- reduces small file issues
- simplifies maintenance
- enhances overall query efficiency

### Impact
- optimizeWrite enabled
- autoCompact enabled
- reduced need for manual optimization operations

---

## Decision 036 — Standardized Logging for Processing Notebooks

### Decision
Adopt standardized logging for all production notebooks.

### Rationale
- improves observability and traceability
- standardizes execution outputs
- facilitates debugging and monitoring

### Impact
- consistent execution logs across Bronze, Silver, and Gold notebooks
- clear visibility into processing steps and results
- improved operational reliability

---

## Decision 037: Analytics Layer Strategy

### Context

During the development of the Analytics layer, exploratory analysis was conducted to evaluate the ability of existing datasets (fact and marts) to support common business questions.

The analysis revealed that:

- the `fact_sales` table provides full analytical flexibility but requires higher computational cost  
- existing marts support specific analytical views but do not fully cover multi-dimensional analysis across customer, product, and channel  

---

### Decision

A single analytics dataset will be created for the current phase:

- `analytics_sales_overview`

No additional analytics datasets will be introduced at this stage.

---

### Rationale

The selected dataset:

- supports multi-dimensional analysis across customer, product, channel, and time  
- covers the majority of business use cases identified during exploratory analysis  
- reduces dependency on the fact table for recurring analytical queries  
- improves query performance and cost efficiency  

Creating additional datasets at this stage would:

- introduce unnecessary duplication  
- increase maintenance complexity  
- provide limited additional value without real usage demand  

---

### Strategy

The Analytics layer follows a usage-driven approach:

- new datasets will only be created if justified by:
  - recurring BI or reporting use cases  
  - performance bottlenecks  
  - need for different analytical grain  

- existing datasets should be reused whenever possible  

---

### Implications

- `analytics_sales_overview` becomes the primary dataset for analytical consumption  
- BI tools and dashboards should prioritize this dataset over direct fact table access  
- future optimization efforts should focus on this dataset  

---

### Future Considerations

Additional analytics datasets may be introduced if:

- new business requirements emerge  
- query performance becomes a constraint  
- specialized analytical views are needed (e.g., customer lifecycle, time-series trends)  

Any new dataset must be justified and documented before implementation.