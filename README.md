# PT Frozen Foods — Modern Data Platform

## Overview

PT Frozen Foods is a real-world, end-to-end data platform project.

Due to privacy and confidentiality constraints, the project uses synthetic datasets and a fictional company name. However, the platform design, architecture, and implementation reflect real enterprise use cases.

The project demonstrates how a modern data platform is structured, orchestrated, and operated end-to-end.

It covers:

- Data Engineering  
- Data Analytics  
- Machine Learning (foundation)  
- Cloud Architecture (Azure)  
- Lakehouse architecture design  

---

## Key Highlights

- End-to-end data pipeline (RAW → Bronze → Silver → Gold)  
- Fully orchestrated with Azure Data Factory  
- Distributed processing using Databricks (Spark + Delta Lake)  
- Infrastructure provisioned via Terraform (IaC)  
- Modular and scalable architecture  
- Repository structured for production-grade maintainability  

---

## Architecture

The platform follows a Lakehouse architecture built on Azure services:

- Azure Data Lake Storage Gen2 (ADLS Gen2)  
- Azure Data Factory (ADF)  
- Azure Databricks (DBX)  
- Azure Key Vault  
- Azure Monitor / Log Analytics  
- Azure Logic Apps  

---

## High-Level Data Flow

Data Sources (Simulated)  
→ Logic Apps / Manual Upload  
→ ADLS Gen2 (RAW Layer)  
→ ADF (Orchestration)  
→ Databricks (Processing)  
→ Bronze → Silver → Gold  
→ Analytics / Machine Learning  

---

## Infrastructure (IaC)

All infrastructure is provisioned using Terraform.

This enables:

- reproducibility of environments  
- version-controlled infrastructure  
- modular design  
- scalability and maintainability  
- readiness for CI/CD  

---

## Data Ingestion Strategy

The ingestion layer combines:

- Azure Logic Apps for automated ingestion scenarios  
- Manual ingestion into RAW to simulate enterprise systems  

This approach allows the platform to represent real-world data ingestion patterns such as:

- CRM systems  
- ERP platforms  
- APIs  
- external operational data sources  

---

## Orchestration Layer (ADF)

Azure Data Factory is responsible for:

- pipeline orchestration  
- dependency management across layers  
- execution scheduling  
- triggering Databricks workloads  

---

## Data Processing (Databricks)

Processing is implemented using Spark and Delta Lake.

Layers:

- Bronze → ingestion and initial structuring  
- Silver → cleansing, validation, and integration  
- Gold → business-ready datasets  

---

## Data Model

The analytical model follows a dimensional approach.

### Dimensions

- dim_product  
- dim_customer  
- dim_supplier  
- dim_sales_channel  
- dim_calendar  
- dim_weather  

### Fact Tables

- fact_sales  

### Data Marts

- mart_customer_sales  
- mart_sales_performance  
- mart_customer_product_mix  

---

## Analytics Layer

The Gold layer enables:

- KPI analysis  
- reporting and dashboards  
- business insights  
- data-driven decision making  

---

## Machine Learning (Next Step)

Planned use cases:

- demand forecasting  
- product recommendation  
- customer segmentation and churn  

---

## Project Structure

pt_frozen_foods_251201/

├── 01_docs/  
├── 02_infra/  
├── 03_data/  
├── 04_notebooks/  
├── 05_src/  
├── 06_outputs/  
└── 07_tests/  

---

## Design Principles

- Git as single source of truth  
- clear separation of concerns  
- layered architecture (Medallion)  
- modular and scalable design  
- performance-oriented processing  
- reproducibility and governance  

---

## Disclaimer

This project uses synthetic data and a fictional company name due to privacy constraints.

No real company data is used.