### Unity Catalog Configuration — PT Frozen Foods

#### Overview

This document describes the Unity Catalog configuration used in the PT Frozen Foods data platform.

Unity Catalog is responsible for centralized data governance, access control, and storage abstraction across the Lakehouse architecture.

---

#### Metastore

| Property | Value |
|----------|-------|
| Name | metastore-ptfrozenfoods-dev |
| Region | West Europe |
| Status | Active |

The metastore is linked to the Databricks workspace and governs all data access.

---

#### Catalog

| Catalog | Purpose |
|---------|--------|
| ptfrozenfoods_dev | Development environment |

---

#### Schemas

| Schema | Description |
|--------|-------------|
| bronze | Raw structured ingestion layer |
| silver | Cleansed and integrated data |
| gold | Analytical and curated datasets |

System schemas:
- default
- information_schema

---

#### Data Hierarchy

Unity Catalog follows a 3-level structure:

Catalog → Schema → Table

Example:

ptfrozenfoods_dev.gold.fact_sales

---

#### Storage Credential

| Property | Value |
|----------|-------|
| Name | sc-ptfrozenfoods-dev |
| Type | Managed Identity |
| Access Connector | ac-ptfrozenfoods-rmds-dev-we |

Used to authenticate access to ADLS via the Access Connector.

---

#### External Locations

| Name | Layer | Purpose |
|------|------|--------|
| el-ptfrozenfoods-dev | raw | raw ingestion |
| el-ptfrozenfoods-bronze-dev | bronze | bronze storage |
| el-ptfrozenfoods-silver-dev | silver | silver storage |
| el-ptfrozenfoods-gold-dev | gold | gold storage |

External Locations are mapped directly to ADLS containers.

---

#### Governance Model

Access is controlled via:

- GRANT statements
- Schema-level permissions
- External Location permissions

Key permissions used:

- USE CATALOG
- USE SCHEMA
- SELECT
- MODIFY
- CREATE TABLE
- READ FILES
- WRITE FILES
- CREATE EXTERNAL TABLE
- MANAGE (required for DROP/ALTER operations)

---

#### ADF Integration

ADF executes notebooks using a service principal:

- Name: adf-ptfrozenfoods-rmds-dev-we
- Identification: GUID (required for GRANTs)

Important behavior:

- Principal must be referenced using **Application ID (GUID)**
- Friendly name does not work in GRANT statements

---

#### Critical Learnings

- Unity Catalog requires both:
  - logical permissions (catalog/schema)
  - physical permissions (external locations)
- Missing permissions lead to execution failures in ADF
- MANAGE permission is required for:
  - DROP TABLE
  - ALTER TABLE
- External Locations must include:
  - READ FILES
  - WRITE FILES
  - CREATE EXTERNAL TABLE

---

#### Role in the Architecture

Unity Catalog acts as the governance layer:

Databricks → Unity Catalog → External Locations → ADLS

It ensures:

- controlled data access
- environment consistency
- secure integration with ADF

---

#### Notes

- All permissions are defined in `02_infra/02_databricks/00_setup`
- Access is fully governed through Unity Catalog
- Direct storage access is not used in the project

---

#### Conclusion

Unity Catalog is a critical component of the platform, enabling secure, governed, and scalable data access across all layers of the architecture.