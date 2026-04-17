# Authentication & Authorization — PT Frozen Foods

## Technical Context

The PT Frozen Foods data platform is built using a modern Lakehouse architecture on Azure, integrating:

* Azure Data Factory (ADF)
* Azure Databricks
* Unity Catalog (UC)
* Azure Data Lake Storage (ADLS)

The platform requires a robust and structured authentication and authorization model to ensure secure and reliable data processing across all layers (Bronze, Silver, Gold).

---

## Identity Strategy

The orchestration layer (ADF) uses a Managed Identity to authenticate against Databricks and Unity Catalog.

* Identity Name: `adf-ptfrozenfoods-rmds-dev-we`
* Application ID: `83f60c2e-b9b4-4bf1-b1ad-5f920c5a21af`

Due to Unity Catalog requirements, all permissions must reference the **Application ID (GUID)** rather than the display name.

---

## Security Model

The platform implements a two-layer security model:

### Unity Catalog (Logical Layer)

Controls access to:

* catalogs
* schemas
* tables

### External Locations (Physical Layer)

Controls access to:

* storage paths (abfss://)
* raw and processed data files

This separation ensures governance at both metadata and storage levels.

---

## Authorization Challenges

During implementation, the following challenges were identified:

* service principal not recognized in Unity Catalog (GUID requirement)
* lack of catalog-level access (`USE CATALOG`)
* insufficient schema-level permissions
* missing access to external locations
* inability to read/write files in ADLS

These issues are common when integrating ADF with Unity Catalog-based Databricks environments.

---

## Role of Permissions

Permissions are required to support the full data pipeline execution:

### Data Processing Requirements

* reading data across layers (Bronze → Silver → Gold)
* writing Delta tables
* recreating tables (DROP + CREATE)
* accessing storage paths
* running Auto Loader

### Required Unity Catalog Permissions

* USE CATALOG
* USE SCHEMA
* SELECT
* MODIFY
* CREATE TABLE

### Required External Location Permissions

* READ FILES
* WRITE FILES
* CREATE EXTERNAL TABLE

---

## External Locations Structure

The platform uses dedicated external locations per layer:

* el-ptfrozenfoods-dev
* el-ptfrozenfoods-bronze-dev
* el-ptfrozenfoods-silver-dev
* el-ptfrozenfoods-gold-dev

This design supports:

* data isolation by layer
* better governance
* scalable security model

---

## Authentication Flow

The execution flow follows this sequence:

* ADF triggers pipeline execution
* Managed Identity authenticates to Databricks
* Unity Catalog validates catalog and schema permissions
* External Location validates storage access
* Notebooks execute data processing

---

## Operational Considerations

* permissions are persistent once granted
* no need to reapply permissions per execution
* errors are typically surfaced in Databricks, not ADF
* troubleshooting requires checking both UC and storage layers

---

## Key Learnings

* always use Application ID for service principals
* Unity Catalog and storage permissions are independent and both required
* external location permissions are critical when using `abfss://` paths
* ADF error codes often mask underlying Databricks issues

---

## Conclusion

The authentication and authorization model is now fully aligned with the Lakehouse architecture, enabling secure, scalable, and reliable execution of the full ADF → Databricks pipeline.

This setup serves as a reusable blueprint for future environments (QA, PROD) and similar data platform implementations.
