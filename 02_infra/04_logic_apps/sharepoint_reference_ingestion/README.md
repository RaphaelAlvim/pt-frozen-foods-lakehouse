# SharePoint Reference Ingestion Logic App

## Purpose

This folder stores the Azure Logic App workflow artifacts related to the automated ingestion of reference datasets from SharePoint into the RAW layer of the PT Frozen Foods platform.

The workflow is responsible for:

- monitoring the SharePoint `reference` folder
- retrieving file content
- validating accepted file names
- storing valid files in RAW
- storing invalid files in `_rejected`
- sending email alerts for rejected files

---

## Files

### `workflow_exported.json`

Raw export taken from the Azure portal.

This file represents the workflow exactly as exported from the running Logic App and should be preserved as the original reference artifact.

### `workflow.json`

Curated version of the Logic App workflow.

This file is the preferred base for repository versioning, review, and future integration with Terraform or other deployment automation.

It may include small cleanups such as:

- corrected email subject
- removal of unused actions
- formatting improvements

---

## Current Behavior

### Valid files

Accepted files:

- `reference_calendar.csv`
- `reference_locations.csv`
- `reference_sales_channels.csv`

Stored at:

- `raw/reference/<dataset>/load_date=YYYY-MM-DD/<dataset>_<timestamp>.csv`

### Rejected files

Any other file name is stored at:

- `raw/reference/_rejected/load_date=YYYY-MM-DD/<filename>_<timestamp>.csv`

Rejected files also trigger an email notification to:

- `rm@rmdatasolutions.net`

---

## Authentication

### SharePoint

- dedicated service account:
  `svc.sharepoint.ingestion@rmdatasolutions.net`

### Blob Storage

- Logic App managed identity

### Email

- SMTP connection configured for operational alerts

---

## Repository Role

This folder separates workflow artifacts from Terraform infrastructure code.

This helps maintain a clean project structure by distinguishing:

- infrastructure provisioning
- workflow logic
- exported operational artifacts

---

## Future Evolution

Planned next steps:

- evaluate integration of workflow JSON into Terraform deployment
- standardize connection handling
- improve environment parameterization
- extend version-controlled automation for Logic Apps