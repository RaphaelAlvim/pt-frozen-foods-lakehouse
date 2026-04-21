### Data Layer Overview

#### Purpose

This document describes the data organization used in the PT Frozen Foods data platform.

It defines how data is structured and stored across different layers in the Data Lake.

---

#### Data Architecture

The platform follows a layered architecture:

    raw → bronze → silver → gold

Additionally:

    exports → external data outputs

---

#### RAW Layer

The RAW layer stores data as received from source systems.

Characteristics:

- no transformation  
- original structure preserved  
- used as ingestion entry point  

Structure:

    raw/
        crm/
        erp/
        reference/
        weather_api/
        web/

---

#### Bronze Layer

The Bronze layer contains structured data ingested from RAW.

Characteristics:

- schema applied  
- stored as Delta tables  
- minimal transformation  

---

#### Silver Layer

The Silver layer contains cleaned and standardized data.

Characteristics:

- data quality applied  
- standardized formats  
- integrated datasets  

---

#### Gold Layer

The Gold layer contains business-ready data.

Characteristics:

- dimensional model (facts and dimensions)  
- optimized for analytics  
- used by BI and ML  

---

#### Exports Layer

The exports layer contains data prepared for external use.

Examples:

- data extracts  
- integrations  
- reporting outputs  

---

#### Source Systems

The platform integrates multiple data sources:

CRM

- clients  
- segmentation  
- status  

ERP

- orders  
- order_items  
- products  
- salespersons  
- suppliers  

Reference Data

- calendar  
- locations  
- sales channels  

External Sources

- weather API  
- web logs  

---

#### Design Principles

- clear separation of layers  
- preservation of raw data  
- progressive data refinement  
- scalability and traceability  

---

#### RAW Data Origin

In this project, RAW data is generated using synthetic data scripts located in:

    05_src/

This simulates real-world source systems such as:

- ERP systems  
- CRM systems  
- external APIs  

The CSV files stored in the repository represent initial datasets used for development and testing purposes.

---

#### Notes

- raw data is stored in original format  
- transformations are handled in Databricks  
- structure is aligned with the Medallion Architecture  
