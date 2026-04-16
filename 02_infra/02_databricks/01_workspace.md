# Azure Databricks Workspace

## Overview

This document describes the Azure Databricks Workspace configured for the **PT Frozen Foods** project. The environment was provisioned following enterprise best practices for security, governance, and scalability within Microsoft Azure.

---

## Workspace Details

| Property | Value |
|----------|-------|
| Status | Active |
| Workspace Name | ptfrozenfoods-dbx-dev |
| Resource Group | rg-ptfrozenfoods-dev-we |
| Location | West Europe |
| Subscription | rmds-ptfrozenfoods-dev |
| Subscription ID | xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx |
| Workspace Type | Hybrid |
| Managed Resource Group | rg-ptfrozenfoods-rmds-db-managed-dev-we |
| Workspace URL | https://adb-xxxxxxxxxxxxxxxx.x.azuredatabricks.net |
| Pricing Tier | Premium |
| Role-Based Access Control (RBAC) | Enabled |
| No Public IP (NPIP) | Enabled |

---

## Security and Governance

The workspace is configured with advanced security and governance features:

- **Premium Tier**: Supports Role-Based Access Control (RBAC) and Unity Catalog.
- **No Public IP (NPIP)**: Prevents direct exposure to the public internet.
- **Managed Resource Group**: Automatically managed by Azure Databricks.
- **Azure Active Directory Integration**: Ensures secure identity and access management.
- **Unity Catalog Compatibility**: Enables centralized data governance and access control.
- **Secure Cluster Connectivity**: Enhances network security.

---

## Networking Configuration

| Configuration | Status |
|---------------|--------|
| Public IP Access | Disabled |
| Secure Cluster Connectivity | Enabled |
| Private Networking | Enabled |
| Managed Identity | Configured via Azure Databricks Access Connector |

---

## Integration with Azure Services

| Service | Purpose |
|---------|---------|
| Azure Data Lake Storage Gen2 | Data storage |
| Azure Databricks | Data processing and analytics |
| Azure Data Factory | Pipeline orchestration |
| Azure Key Vault | Secrets management |
| Azure Log Analytics | Monitoring and observability |
| Unity Catalog | Data governance and access control |
| Terraform | Infrastructure provisioning (IaC) |
| Power BI | Business intelligence and data visualization |

---

## Role in the PT Frozen Foods Architecture

Azure Databricks plays a central role in the project's Lakehouse architecture:

- Processing and transforming data.
- Implementing the Medallion Architecture (Bronze, Silver, and Gold layers).
- Executing analytical notebooks and data pipelines.
- Creating and managing Delta Lake tables.
- Preparing curated datasets for Business Intelligence and Machine Learning.

---

## Architectural Context

```
Azure Data Factory
        │
        ▼
Azure Databricks
        │
        ▼
Unity Catalog
        │
        ▼
Azure Data Lake Storage Gen2
        │
        ▼
Power BI
```

---

## Notes

- Sensitive information has been masked for public portfolio publication.
- No credentials or confidential identifiers are exposed in this document.
- The environment follows Microsoft best practices for secure and scalable analytics solutions.

---

## References

- https://learn.microsoft.com/azure/databricks/
- https://learn.microsoft.com/azure/databricks/unity-catalog/
- https://learn.microsoft.com/azure/architecture/