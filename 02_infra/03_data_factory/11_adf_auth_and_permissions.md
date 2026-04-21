### ADF Authentication and Permissions

#### Purpose

This document describes the authentication and authorization model used by Azure Data Factory (ADF) in the PT Frozen Foods data platform.

It defines how identities are managed and how permissions are applied across Unity Catalog and External Locations.

---

#### Technical Context

The platform is built using:

- Azure Data Factory (ADF)  
- Azure Databricks  
- Unity Catalog (UC)  
- Azure Data Lake Storage (ADLS)  

A structured security model is required to ensure reliable and secure data processing across all layers.

---

#### Identity Strategy

ADF uses a Service Principal (Managed Identity) to authenticate with Databricks and Unity Catalog.

- Identity Name: adf-ptfrozenfoods-rmds-dev-we  
- Application ID: 83f60c2e-b9b4-4bf1-b1ad-5f920c5a21af  

All permissions in Unity Catalog must reference the Application ID (GUID), not the display name.

---

#### Security Model

The platform uses a two-layer security model:

Unity Catalog (logical layer)

- controls access to catalogs, schemas, and tables  
- defines what operations can be performed  

External Locations (physical layer)

- controls access to storage paths (abfss://)  
- defines read/write capabilities in the Data Lake  

Both layers are required for successful execution.

---

#### Required Permissions

To support full pipeline execution, the following permissions are required.

Unity Catalog:

- USE CATALOG  
- USE SCHEMA  
- SELECT  
- MODIFY  
- CREATE TABLE  
- MANAGE  

External Locations:

- READ FILES  
- WRITE FILES  
- CREATE EXTERNAL TABLE  

---

#### External Locations Structure

The platform uses dedicated external locations per layer:

- el-ptfrozenfoods-dev  
- el-ptfrozenfoods-bronze-dev  
- el-ptfrozenfoods-silver-dev  
- el-ptfrozenfoods-gold-dev  

This structure enables:

- data isolation by layer  
- improved governance  
- scalable access control  

---

#### Authentication Flow

The execution flow follows this sequence:

    ADF Trigger
       │
       ▼
    Service Principal Authentication
       │
       ▼
    Unity Catalog Authorization
       │
       ▼
    External Location Authorization
       │
       ▼
    Databricks Notebook Execution

---

#### Common Challenges

During implementation, the following issues were identified:

- service principal not recognized without Application ID  
- missing USE CATALOG permission  
- insufficient schema-level permissions  
- lack of access to external locations  
- storage access failures (abfss://)  

---

#### Operational Considerations

- permissions are persistent once granted  
- no need to reapply permissions per execution  
- errors are often surfaced in Databricks, not ADF  
- troubleshooting requires checking both UC and storage layers  

---

#### Key Learnings

- always use Application ID for service principals  
- Unity Catalog and storage permissions are independent  
- both layers must be correctly configured  
- external location permissions are critical  
- ADF errors may hide underlying Databricks issues  

---

#### Notes

- this model is aligned with enterprise best practices  
- reusable across environments (dev, qa, prod)  
- supports secure and scalable pipeline execution  
