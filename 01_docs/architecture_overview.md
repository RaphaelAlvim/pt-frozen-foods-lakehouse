# Architecture Overview — PT Frozen Foods

## 1. Overview

This project implements a modern data platform based on a Lakehouse architecture using Azure services.

The goal is to simulate a real enterprise-grade data environment while using synthetic data and a fictional company name due to confidentiality constraints.

The architecture is designed to reflect real-world practices in data engineering, including separation of concerns, scalability, and modularity.

---

## 2. Design Principles

The platform is built following key architectural principles:

- Separation of concerns (ingestion, orchestration, processing, analytics)
- Modular and scalable architecture
- Cloud-native design
- Infrastructure as Code (IaC)
- Reproducibility and version control
- Realistic simulation of enterprise data workflows

---

## 3. High-Level Architecture

The platform follows a layered Lakehouse model:

Data Sources (Simulated)  
-> Logic Apps / Manual Upload  
-> ADLS Gen2 (RAW)  
-> ADF (Orchestration)  
-> Databricks (Processing)  
-> Bronze -> Silver -> Gold  
-> Analytics / Machine Learning  

---

## 4. Data Ingestion Layer

Data ingestion is implemented using a hybrid approach:

- Azure Logic Apps for automated ingestion scenarios (e.g., SharePoint integration)
- Manual uploads to the RAW layer for controlled simulation of enterprise data sources

Manual ingestion is intentionally used to simulate systems such as CRM, ERP, APIs, and external providers, since real production data cannot be used due to confidentiality constraints.

This design allows flexibility while preserving a realistic ingestion pattern.

---

## 5. Storage Layer (Lakehouse)

The platform uses Azure Data Lake Storage Gen2 as the central storage layer.

Data is organized into the following zones:

- RAW: landing zone for incoming data
- Bronze: structured and standardized data
- Silver: cleaned and enriched data
- Gold: curated data for analytics and ML

Delta Lake format is used in processed layers to enable:

- ACID transactions
- schema evolution
- time travel
- improved performance

---

## 6. Orchestration Layer

Azure Data Factory is responsible for orchestration.

In this project, ADF is used to:

- trigger Databricks notebooks
- manage pipeline execution
- coordinate dependencies between layers

Although ingestion is partially handled outside ADF (Logic Apps and manual uploads), in a real-world scenario ADF would also orchestrate ingestion from multiple enterprise systems.

---

## 7. Processing Layer (Databricks)

Azure Databricks is used for data processing.

Key responsibilities:

- transforming RAW data into Bronze, Silver, and Gold layers
- implementing business logic
- performing aggregations and feature engineering

Processing is implemented using Spark and Delta Lake.

---

## 8. Analytics Layer

The Gold layer provides data for:

- business intelligence
- reporting
- advanced analytics
- machine learning models

This layer is structured to support both batch analytics and future real-time extensions.

---

## 9. Infrastructure Layer (Terraform)

All infrastructure is provisioned using Terraform.

The infrastructure follows a modular design, including:

- resource_group
- storage_account (ADLS Gen2)
- key_vault
- monitoring (Log Analytics)
- data_factory
- databricks_workspace (planned)
- logic_app (planned)

Benefits of this approach:

- consistent environment provisioning
- reduced human error
- version-controlled infrastructure
- scalability and maintainability
- readiness for CI/CD integration

---

## 10. Observability and Monitoring

Monitoring is implemented using Azure Log Analytics.

This enables:

- centralized logging
- pipeline monitoring
- error tracking
- performance analysis

Future enhancements may include alerting and dashboards.

---

## 11. Future Enhancements

The architecture is designed to evolve.

Planned improvements include:

- full Databricks integration with ADF pipelines
- automated ingestion using Logic Apps
- alerting and monitoring enhancements
- CI/CD pipelines for infrastructure and data workflows
- real-time data processing capabilities
