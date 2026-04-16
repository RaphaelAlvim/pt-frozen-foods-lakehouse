# Unity Catalog Configuration

## Overview

This document describes the Unity Catalog configuration for the **PT Frozen Foods** project. Unity Catalog provides centralized governance, fine-grained access control, and data lineage across the Azure Databricks environment.

It ensures secure, scalable, and compliant management of data assets within the Lakehouse architecture implemented on Microsoft Azure.

---

## Metastore Information

| Property | Value |
|----------|-------|
| Metastore Name | metastore-ptfrozenfoods-dev |
| Region | West Europe |
| Cloud Provider | Microsoft Azure |
| Status | Active |
| Metastore ID | xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx |
| Metastore Root Storage | abfss://<container>@<storage-account>.dfs.core.windows.net/ |

> **Note:** Sensitive identifiers have been masked for security purposes.

---

## Catalog Configuration

| Catalog Name | Purpose |
|--------------|---------|
| ptfrozenfoods_dev | Development environment for the PT Frozen Foods data platform |

---

## Schema Organization

The catalog is structured according to the Medallion Architecture.

| Schema | Description |
|--------|-------------|
| bronze | Raw structured data ingested from source systems |
| silver | Cleansed, standardized, and integrated datasets |
| gold | Curated and analytics-ready data models |
| default | Default schema automatically created by Databricks |
| information_schema | System schema containing metadata and governance information |

---

## Data Governance Hierarchy

Unity Catalog organizes data using a three-level namespace:

```
Catalog → Schema → Table/View
```

### Example

```
ptfrozenfoods_dev.gold.fact_sales
```

---

## Storage Credential

The Unity Catalog uses a Managed Identity through the Azure Databricks Access Connector to securely access Azure Data Lake Storage Gen2.

| Property | Value |
|----------|-------|
| Storage Credential Name | sc-ptfrozenfoods-dev |
| Authentication Type | Managed Identity |
| Access Connector | ac-ptfrozenfoods-rmds-dev-we |
| Cloud Storage | Azure Data Lake Storage Gen2 |

---

## External Locations

External locations provide secure access to data stored in ADLS Gen2.

| External Location | Storage Credential | Storage Path |
|-------------------|-------------------|--------------|
| el-ptfrozenfoods-dev | sc-ptfrozenfoods-dev | abfss://raw@<storage-account>.dfs.core.windows.net/ |
| el-ptfrozenfoods-bronze-dev | sc-ptfrozenfoods-dev | abfss://bronze@<storage-account>.dfs.core.windows.net/ |
| el-ptfrozenfoods-silver-dev | sc-ptfrozenfoods-dev | abfss://silver@<storage-account>.dfs.core.windows.net/ |
| el-ptfrozenfoods-gold-dev | sc-ptfrozenfoods-dev | abfss://gold@<storage-account>.dfs.core.windows.net/ |

---

## Unity Catalog Managed Storage

The root storage location for the Unity Catalog metastore is configured as follows:

| Property | Value |
|----------|-------|
| External Location Name | metastore_root_location |
| Storage Credential ID | xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx |
| Storage Path | abfss://dbx-uc@<storage-account>.dfs.core.windows.net/|
| Description | Auto-created external location for Unity Catalog managed storage |

---

## Security and Access Control

Unity Catalog enables centralized and fine-grained data governance.

| Feature | Status |
|---------|--------|
| Centralized Governance | Enabled |
| Role-Based Access Control (RBAC) | Enabled |
| Fine-Grained Permissions | Enabled |
| Data Lineage | Enabled |
| Data Auditing | Enabled |
| Managed Identity Authentication | Enabled |
| External Data Access | Enabled |

---

## Supported Data Layers

The Unity Catalog supports the Medallion Architecture implemented in the PT Frozen Foods platform.

| Layer | Container | Purpose |
|-------|-----------|---------|
| RAW | raw | Raw data ingestion |
| Bronze | bronze | Incrementally ingested structured data |
| Silver | silver | Cleansed and standardized data |
| Gold | gold | Curated, analytics-ready datasets |

---

## Architectural Context

```text
Azure Databricks
        │
        ▼
Unity Catalog
        │
        ▼
Storage Credentials
        │
        ▼
External Locations
        │
        ▼
Azure Data Lake Storage Gen2
```

---

## Best Practices Implemented

- Centralized governance using Unity Catalog.
- Secure authentication via Managed Identity.
- Use of Azure Databricks Access Connector.
- Fine-grained access control through RBAC.
- External locations mapped to Medallion Architecture layers.
- Separation of environments and data domains.
- Compliance with Microsoft and Databricks security standards.
- Infrastructure managed using Terraform (IaC).
- Alignment with Lakehouse architectural principles.

---

## Notes

- Sensitive identifiers such as Metastore IDs and Credential IDs have been masked.
- The configuration follows Microsoft Azure and Databricks best practices.
- This setup supports a scalable, secure, and enterprise-grade data platform.
- All resources are deployed in the West Europe region.

---

## References

- https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/
- https://learn.microsoft.com/azure/databricks/connect/unity-catalog/cloud-storage/
- https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/azure-managed-identities/
- https://learn.microsoft.com/azure/databricks/lakehouse/
- https://learn.microsoft.com/azure/architecture/