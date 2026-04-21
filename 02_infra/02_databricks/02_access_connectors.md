### Databricks Access Connector — PT Frozen Foods

#### Overview

This document describes the Azure Databricks Access Connector used in the PT Frozen Foods data platform.

The Access Connector enables secure access from Databricks to Azure Data Lake Storage (ADLS Gen2) using Managed Identity, supporting Unity Catalog governance and eliminating the need for secrets.

---

#### Configuration

| Property | Value |
|----------|-------|
| Name | ac-ptfrozenfoods-rmds-dev-we |
| Resource Group | rg-ptfrozenfoods-dev-we |
| Location | West Europe |
| Subscription | rmds-ptfrozenfoods-dev |
| Identity Type | System-Assigned |

---

#### Authentication Model

The Access Connector uses a **system-assigned Managed Identity** to authenticate Databricks access to storage.

Key characteristics:

- No secrets or keys are required
- Authentication is handled by Azure
- Identity is automatically managed

---

#### Storage Access

The Managed Identity is granted access to the Data Lake via RBAC:

| Role | Scope |
|------|-------|
| Storage Blob Data Contributor | ADLS Gen2 |

This allows Databricks to:

- Read and write data
- Create and manage Delta tables
- Support ingestion and transformations

---

#### Integration with Unity Catalog

The Access Connector is used indirectly through Unity Catalog:

- It backs the **Storage Credential**
- Storage Credentials are used by **External Locations**
- External Locations map to ADLS paths

Logical flow:

Databricks → Access Connector → Storage Credential → External Location → ADLS

---

#### External Locations

The following External Locations are configured using this connector:

- el-ptfrozenfoods-dev
- el-ptfrozenfoods-bronze-dev
- el-ptfrozenfoods-silver-dev
- el-ptfrozenfoods-gold-dev

These locations correspond to the Medallion layers.

---

#### Role in the Architecture

The Access Connector enables:

- Secure access to ADLS from Databricks
- Integration with Unity Catalog governance
- Execution of Bronze, Silver and Gold pipelines
- Controlled access via GRANTs on External Locations

---

#### Provisioning

- Provisioned via Terraform
- No manual credential configuration required
- Fully aligned with Infrastructure as Code principles

---

#### Notes

- All access to storage is mediated by Unity Catalog
- Permissions are enforced via:
  - GRANT statements
  - External Location privileges
- Direct access to storage is not used in the project

---

#### Conclusion

The Access Connector is a foundational component that enables secure, governed, and scalable data access between Databricks and ADLS.

It ensures that all data interactions follow Unity Catalog governance and Azure security best practices.