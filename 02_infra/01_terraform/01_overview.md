### Terraform Overview

#### Purpose

This document describes how infrastructure is provisioned in the PT Frozen Foods data platform using Terraform.

---

#### Scope

Terraform is used to provision all core Azure resources required by the platform.

This includes:

- Resource Group  
- Azure Data Lake Storage Gen2  
- Azure Databricks Workspace  
- Azure Databricks Access Connector  
- Azure Data Factory  
- Azure Logic Apps  
- Azure Key Vault  
- Azure Log Analytics  

---

#### Approach

Infrastructure is defined as code (IaC) using Terraform modules.

This ensures:

- reproducibility  
- version control  
- environment consistency  
- modular and reusable infrastructure  

---

#### Architecture Overview

The infrastructure is organized using modular components:

    Terraform Root
        │
        ├── Resource Group
        ├── Storage Account (ADLS)
        ├── Key Vault
        ├── Log Analytics (Monitoring)
        ├── Data Factory
        ├── Databricks Workspace
        ├── Access Connector
        ├── Role Assignments
        └── Logic App

---

#### Modules

The following modules are used:

- resource_group  
- storage_account  
- key_vault  
- monitoring  
- data_factory  
- databricks_workspace  
- access_connector  
- role_assignments  
- logic_app  

Each module is responsible for provisioning a specific Azure resource.

---

#### Key Resources

Resource Group

- central container for all resources  

Storage Account (ADLS Gen2)

- stores data across raw, bronze, silver, and gold layers  

Key Vault

- manages secrets and secure configuration  

Log Analytics

- supports monitoring and observability  

Data Factory

- orchestrates data pipelines  

Databricks Workspace

- executes data processing and analytics  

Access Connector

- enables secure access between Databricks and ADLS via Managed Identity  

Role Assignments

- grants required permissions to the Access Connector  

Logic App

- supports ingestion of reference data (e.g., SharePoint integration)  

---

#### Identity and Security

- Managed Identity is used for secure authentication  
- Access Connector is used to integrate Databricks with ADLS  
- Role assignments grant storage access to Databricks  

---

#### Environment Strategy

The platform is currently deployed in:

- environment: dev  
- region: West Europe  

Future environments may include:

- qa  
- prod  

---

#### Integration with Platform

Terraform provisions the infrastructure that enables:

- data ingestion  
- data processing  
- orchestration  
- monitoring  
- governance  

---

#### Notes

- infrastructure is fully modular and reusable  
- all resources are provisioned via Terraform modules  
- configuration follows Azure best practices  
- sensitive data is not exposed in the repository  