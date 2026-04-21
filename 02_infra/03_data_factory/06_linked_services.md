### Linked Services

#### Purpose

This document describes the Linked Services configured in Azure Data Factory (ADF) for the PT Frozen Foods data platform.

Linked Services define the connections between ADF and external services used in the architecture.

---

#### Linked Service List

##### ls_databricks_ptfrozenfoods_dev

Primary Linked Service used to connect Azure Data Factory to Azure Databricks.

---

#### Configuration Details

- Service Type: Azure Databricks  
- Workspace: ptfrozenfoods-dbx-dev  
- Authentication: Azure Active Directory (Service Principal)  
- Service Principal: adf-ptfrozenfoods-rmds-dev-we  
- Region: West Europe  

---

#### Purpose in the Project

This Linked Service is responsible for:

- executing Databricks notebooks  
- orchestrating data processing pipelines  
- enabling communication between ADF and Databricks  
- supporting execution across Bronze, Silver, and Gold layers  

---

#### Execution Context

All pipelines use this Linked Service to:

- trigger notebook execution  
- pass parameters (if applicable)  
- monitor execution status  

---

#### Integration with Pipelines

The following pipelines use this Linked Service:

- pl_bronze_ingestion  
- pl_silver_standardization  
- pl_silver_integration  
- pl_gold_dimensions  
- pl_gold_fact  
- pl_gold_marts  
- pl_ptfrozenfoods_master  

---

#### Security and Authentication

Authentication is handled via Service Principal:

- identity is registered in Azure Active Directory  
- permissions are granted via Unity Catalog and External Locations  
- no credentials or secrets are exposed in pipelines  

---

#### Design Principles

- centralized connection management  
- secure authentication via AAD  
- reuse across all pipelines  
- alignment with enterprise best practices  

---

#### Future Extensions

Additional Linked Services may include:

- Azure Data Lake Storage Gen2  
- Azure SQL Database  
- REST APIs  
- external data sources  

---

#### Notes

- current configuration uses a single Linked Service  
- sufficient for current architecture  
- scalable for future integrations  
