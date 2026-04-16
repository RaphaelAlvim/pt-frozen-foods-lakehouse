# Azure Databricks Access Connector

## Overview

This document describes the Azure Databricks Access Connector configured for the **PT Frozen Foods** project. The Access Connector enables secure and seamless connectivity between Azure Databricks and Azure Data Lake Storage Gen2 (ADLS Gen2) using Managed Identity, eliminating the need for secrets or storage keys.

---

## Access Connector Details

| Property | Value |
|----------|-------|
| Access Connector Name | ac-ptfrozenfoods-rmds-dev-we |
| Resource Group | rg-ptfrozenfoods-dev-we |
| Location | West Europe |
| Subscription | rmds-ptfrozenfoods-dev |
| Subscription ID | xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx |
| State | Succeeded |
| Resource ID | /subscriptions/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx/resourceGroups/rg-ptfrozenfoods-dev-we/providers/Microsoft.Databricks/accessConnectors/ac-ptfrozenfoods-rmds-dev-we |
| Managed Identity Type | System-Assigned |
| Managed Identity Principal ID | xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx |

---

## Security and Access Control

The Access Connector uses a system-assigned Managed Identity to securely authenticate with Azure services.

### Role-Based Access Control (RBAC)

| Role | Scope | Purpose |
|------|-------|---------|
| Storage Blob Data Contributor | Azure Data Lake Storage Gen2 | Enables read and write access to data in storage containers. |

This role ensures that Azure Databricks can securely access and manage data stored in ADLS Gen2.

---

## Integration with Azure Databricks

The Access Connector is linked to the Azure Databricks Workspace and is used to authenticate access to external storage resources.

### Supported Use Cases

- Secure access to Azure Data Lake Storage Gen2.
- Integration with Unity Catalog.
- Creation of Storage Credentials.
- Configuration of External Locations.
- Governance and fine-grained access control.
- Secure data ingestion and transformation.

---

## Integration with Unity Catalog

The Access Connector plays a critical role in enabling data governance through Unity Catalog.

### Functional Architecture

```
Azure Databricks
        │
        ▼
Access Connector (Managed Identity)
        │
        ▼
Storage Credential (Unity Catalog)
        │
        ▼
External Location (Unity Catalog)
        │
        ▼
Azure Data Lake Storage Gen2
```

---

## Storage Scope

The Access Connector provides secure access to the project's Data Lake containers:

- **raw**
- **bronze**
- **silver**
- **gold**

These containers support the Medallion Architecture implemented in the PT Frozen Foods platform.

---

## Provisioning and Governance

| Component | Configuration |
|-----------|--------------|
| Provisioning Method | Terraform (Infrastructure as Code) |
| Authentication Method | Managed Identity |
| Secrets Management | Not Required |
| Governance Model | Unity Catalog |
| Compliance | Azure Security Best Practices |

---

## Role in the PT Frozen Foods Architecture

The Access Connector ensures secure, scalable, and governed access between Azure Databricks and Azure Storage. It supports enterprise-grade data governance and aligns with Microsoft best practices.

### Key Benefits

- Eliminates the need for storage keys and secrets.
- Enhances security through Managed Identity.
- Enables seamless integration with Unity Catalog.
- Supports centralized access control using RBAC.
- Aligns with Zero Trust security principles.
- Facilitates scalable and maintainable data architectures.

---

## References

- https://learn.microsoft.com/azure/databricks/connect/unity-catalog/cloud-storage/azure-managed-identities
- https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/azure-managed-identities
- https://learn.microsoft.com/azure/role-based-access-control/
- https://learn.microsoft.com/azure/storage/blobs/data-lake-storage-introduction
- https://learn.microsoft.com/azure/databricks/