# PT Frozen Foods — Data Platform Project

## Overview

PT Frozen Foods is a real data platform project based on a realistic business scenario of a frozen food distribution company located in Northern Portugal.

Due to confidentiality constraints, the project uses a fictional name and synthetic datasets. However, all architectural decisions, business logic, and technical implementations are designed to reflect a real production environment.

The project demonstrates end-to-end capabilities in:

- Data Engineering  
- Data Analytics  
- Machine Learning  
- Cloud Architecture (Azure)  
- Lakehouse design pattern  

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

## Infrastructure (Infrastructure as Code)

All infrastructure is provisioned using **Terraform**, following Infrastructure as Code (IaC) principles.

This approach provides several advantages:

- reduction of human errors  
- environment standardization  
- infrastructure reproducibility  
- easier maintenance and evolution  
- code reuse through modular design  
- version control integration with Git  
- readiness for CI/CD pipelines  

The infrastructure is designed in a modular way, enabling scalability and consistent environment management.

---

## High-Level Data Flow

Data Sources (Simulated)  
-> Logic Apps / Manual Upload  
-> ADLS Gen2 (RAW)  
-> ADF (Orchestration)  
-> Databricks (Processing)  
-> Bronze -> Silver -> Gold  
-> Analytics / Machine Learning  

---

## Data Ingestion Strategy

Data ingestion is implemented using a hybrid approach:

- Azure Logic Apps for selected automated ingestion scenarios (e.g., SharePoint simulation)  
- Manual uploads to the RAW layer of the Data Lake for specific datasets  

Manual ingestion is intentionally used to simulate enterprise data sources such as CRM systems, ERP platforms, APIs, and other operational systems, since the project uses synthetic datasets and real production data cannot be exposed due to confidentiality constraints.

This approach allows the platform to realistically simulate real-world ingestion patterns while maintaining full control over data and ensuring proper governance.

---

## Orchestration Layer

Azure Data Factory is used as the orchestration layer in this project.

Within this portfolio context, ADF is mainly responsible for:

- triggering Databricks notebooks  
- coordinating execution across Bronze, Silver, and Gold layers  
- scheduling and managing transformation pipelines  

In a real production environment, ADF would also orchestrate data ingestion from enterprise systems such as ERP, CRM, APIs, and other external sources, integrating multiple data flows across the platform.

---

## Data Processing Layer

Data processing is executed in Azure Databricks using Spark and Delta Lake.

The processing pipeline follows a layered approach:

- Bronze: structured data after ingestion, with initial standardization  
- Silver: cleaned, validated, and enriched data  
- Gold: business-ready datasets  

---

## Data Model

The analytical model follows a dimensional design.

### Dimensions

- dim_product  
- dim_customer  
- dim_supplier  
- dim_sales_channel  
- dim_calendar  

### Fact Tables

- fact_sales  
- fact_web_events  

---

## Analytics Layer

The Gold layer provides curated datasets for analytical use, enabling:

- performance analysis  
- data exploration  
- dashboards and reporting  
- foundation for advanced analytics and predictive models  

---

## Machine Learning

Planned machine learning use cases include:

- demand forecasting  
- product recommendation  
- customer churn prediction  

---

## Project Structure

pt_frozen_foods_251201/  
├── README.md  
├── requirements.txt  
├── .gitignore  
├── .env.example  
├── 01_docs/  
├── 02_infra/  
├── 03_data/  
├── 04_notebooks/  
├── 05_src/  
├── 06_outputs/  
└── 07_tests/  

---

## Key Design Principles

- separation of concerns (ingestion, orchestration, processing)  
- modular and cloud-native architecture  
- Lakehouse design pattern  
- Infrastructure as Code (IaC)  
- scalability and reproducibility  
- realistic simulation of enterprise data workflows  

---

## Disclaimer

This project represents a real-world data platform scenario but uses synthetic data and a fictional company name for confidentiality reasons.

All architectural decisions, data flows, and implementations are designed to reflect real enterprise practices while ensuring that no sensitive or confidential data is exposed.
