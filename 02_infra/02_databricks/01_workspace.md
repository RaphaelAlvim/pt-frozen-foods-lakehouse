### Databricks Workspace — PT Frozen Foods

#### Overview

This document describes the Azure Databricks Workspace used in the PT Frozen Foods data platform.

The workspace is the central execution environment for data processing, integrated with Unity Catalog for governance and Azure Data Factory for orchestration.

---

#### Workspace Configuration

| Property | Value |
|----------|-------|
| Workspace Name | ptfrozenfoods-dbx-dev |
| Resource Group | rg-ptfrozenfoods-dev-we |
| Location | West Europe |
| Subscription | rmds-ptfrozenfoods-dev |
| Pricing Tier | Premium |
| Managed Resource Group | rg-ptfrozenfoods-rmds-db-managed-dev-we |

---

#### Key Capabilities

- Execution of data processing notebooks (Bronze, Silver, Gold)
- Integration with Unity Catalog for data governance
- Delta Lake processing with optimized storage and performance
- Orchestration via Azure Data Factory (ADF)
- Central compute layer for analytics and data transformation

---

#### Security and Governance

- Unity Catalog enabled for centralized governance
- RBAC enabled at workspace level
- Managed Identity used for secure access to storage
- External Locations configured for controlled data access
- Permissions managed via SQL scripts (`00_setup`)

---

#### Networking

- No Public IP (NPIP) enabled
- Secure cluster connectivity enabled
- Private networking enforced

---

#### Integration in the Architecture

The Databricks Workspace operates as the compute layer in the Lakehouse architecture:

- ADF triggers execution of notebooks
- Databricks processes and transforms data
- Unity Catalog governs access to data assets
- Data is stored in Azure Data Lake (ADLS Gen2)
- Outputs are consumed by BI and future ML workloads

---

#### Repository Integration

The workspace is integrated with the GitHub repository using Databricks Git folders.

Key principles:

- The repository is the **single source of truth**
- The workspace reflects the repository structure
- All production notebooks are executed from Git-aligned paths
- No official notebooks exist outside the Git folder

---

#### Role in the Project

Databricks is responsible for:

- Implementing the Medallion Architecture (Bronze, Silver, Gold)
- Building dimensional models and marts
- Executing production-grade processing notebooks
- Preparing datasets for BI and ML consumption

---

#### Notes

- Sensitive identifiers are masked for portfolio purposes
- Infrastructure is provisioned via Terraform where applicable
- Permissions and environment setup are defined in `02_infra/02_databricks/00_setup`

---

#### Conclusion

The Databricks Workspace is not a source of logic or configuration.

It is an execution environment fully aligned with the repository, ensuring consistency, reproducibility, and maintainability across the data platform.