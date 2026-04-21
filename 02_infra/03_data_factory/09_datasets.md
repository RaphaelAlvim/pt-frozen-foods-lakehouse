### ADF Datasets

#### Purpose

This document describes the datasets used in Azure Data Factory (ADF) for the PT Frozen Foods data platform.

Datasets define the structure and location of data used by pipelines and activities.

---

#### Concept

In ADF, a dataset represents:

- a reference to data  
- the structure of the data  
- the connection via a Linked Service  

Datasets do not store data. They only describe how data is accessed.

---

#### Current Usage

In this project:

- datasets are minimally used  
- most data processing is executed via Databricks notebooks  
- data access is handled directly inside Databricks using Unity Catalog  

---

#### Role in Architecture

Datasets are not the primary data access mechanism.

Instead:

    ADF → triggers Databricks → Databricks reads/writes via Unity Catalog

---

#### When Datasets Are Used

Datasets are required when:

- using Copy Activity  
- moving data between sources  
- integrating external systems  
- working with file-based ingestion directly in ADF  

---

#### Current Design Decision

The platform follows a Databricks-centric approach:

- ADF is used for orchestration only  
- data transformations are handled in Databricks  
- datasets are not heavily used  

---

#### Future Usage

Datasets may be introduced for:

- ingestion from external sources  
- integration with APIs  
- structured data movement using Copy Activity  
- hybrid ingestion scenarios  

---

#### Design Principles

- avoid unnecessary dataset definitions  
- keep ADF focused on orchestration  
- centralize data logic in Databricks  
- use datasets only when required  

---

#### Notes

- current architecture minimizes ADF data handling  
- aligns with Lakehouse best practices  
- reduces duplication between ADF and Databricks logic  
