### ADF Overview

#### Purpose

This document describes the role of Azure Data Factory (ADF) in the PT Frozen Foods data platform.

ADF is responsible for orchestrating data pipelines and executing Databricks notebooks across the Medallion Architecture.

---

#### Role in Architecture

ADF acts as the orchestration layer of the platform.

It is responsible for:

- triggering data pipelines
- executing Databricks notebooks
- managing dependencies between layers
- controlling execution order (bronze → silver → gold)
- enabling automation and scheduling

---

#### Pipelines

The project is structured using a modular pipeline approach:

- pl_bronze_ingestion
- pl_silver_standardization
- pl_silver_integration
- pl_gold_dimensions
- pl_gold_fact
- pl_gold_marts
- pl_ptfrozenfoods_master

---

#### Execution Flow

Master pipeline orchestrates execution:

1. Bronze ingestion
2. Silver standardization
3. Silver integration
4. Gold dimensions
5. Gold fact
6. Gold marts

---

#### Integration with Databricks

ADF executes Databricks notebooks using:

- Linked Service: ls_databricks_ptfrozenfoods_dev
- Notebook path via Git folder
- Execution via job cluster

---

#### Repository Integration

ADF is integrated with Git:

- repository: pt-frozen-foods-lakehouse
- collaboration branch: main
- publish branch: adf_publish

All pipelines are versioned in the repository.

---

#### Notes

- ADF is used only for orchestration
- all transformations are executed in Databricks
- pipelines follow a modular and scalable design
- execution is validated end-to-end via master pipeline

---

#### Ingestion Strategy

In a real production environment, Azure Data Factory (ADF) is also responsible for data ingestion from enterprise data sources.

This includes:

- ERP systems
- CRM systems
- APIs and external services
- operational databases

However, in this project:

- synthetic datasets are used for demonstration purposes
- ingestion is simulated directly within Databricks
- reference data is ingested via Azure Logic Apps (SharePoint integration)

---

#### Role Separation

- Azure Data Factory → orchestration + ingestion (production scenario)
- Azure Logic Apps → ingestion of reference data (SharePoint)
- Azure Databricks → data processing and transformation

---

#### Role in BI and ML

Azure Data Factory does not perform analytics or machine learning directly.

However, it plays a critical role by orchestrating data pipelines and ensuring that datasets are prepared, updated, and available for downstream consumption.

- For Business Intelligence, ADF ensures that Gold layer datasets are refreshed and ready for Power BI.
- For Machine Learning, ADF can orchestrate training and scoring workflows by triggering Databricks notebooks.

ADF acts as the orchestration backbone of the platform.

---

#### Role Separation

- Azure Data Factory → orchestration, ingestion (production), and pipeline coordination across BI and ML workflows
- Azure Logic Apps → ingestion of reference data (SharePoint)
- Azure Databricks → data processing, transformation, and machine learning execution
- Power BI → data visualization and business intelligence consumption