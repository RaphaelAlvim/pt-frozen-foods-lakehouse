### Logic App — SharePoint Reference Ingestion

#### Purpose

This folder contains the Azure Logic App workflow responsible for ingesting reference datasets from SharePoint into the RAW layer of the PT Frozen Foods platform.

---

#### Responsibilities

The workflow is responsible for:

- monitoring the SharePoint reference folder  
- retrieving file content  
- validating accepted file names  
- storing valid files in RAW  
- storing invalid files in _rejected  
- sending email alerts for rejected files  

---

#### Files

workflow_exported.json

- raw export from Azure portal  
- represents the exact deployed workflow  
- preserved as source reference  

workflow.json

- curated version for repository usage  
- used for versioning and future automation  
- may include cleanup and formatting improvements  

---

#### Current Behavior

Valid files

Accepted files:

- reference_calendar.csv  
- reference_locations.csv  
- reference_sales_channels.csv  

Storage path:

    raw/reference/<dataset>/load_date=YYYY-MM-DD/<dataset>_<timestamp>.csv  

---

Rejected files

Any non-matching file is stored at:

    raw/reference/_rejected/load_date=YYYY-MM-DD/<filename>_<timestamp>.csv  

Rejected files trigger an email notification to:

- rm@rmdatasolutions.net  

---

#### Authentication

SharePoint

- service account: svc.sharepoint.ingestion@rmdatasolutions.net  

Storage

- Logic App managed identity  

Email

- SMTP connection configured for alerts  

---

#### Role in Architecture

The Logic App is responsible for reference data ingestion.

    SharePoint
        │
        ▼
    Logic App
        │
        ▼
    RAW (ADLS)

This complements the ADF orchestration layer.

---

#### Repository Role

This folder separates:

- infrastructure provisioning (Terraform)  
- workflow logic (Logic App)  
- operational artifacts (exports)  

---

#### Future Evolution

- integrate workflow deployment with Terraform  
- standardize connection handling  
- introduce parameterization  
- extend automation and CI/CD support  

---

#### Notes

- workflow is currently stable and operational  
- supports ingestion of reference datasets  
- aligned with the overall Lakehouse architecture  