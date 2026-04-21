### Monitoring Overview

#### Purpose

This document describes the monitoring strategy used in the PT Frozen Foods data platform.

It defines how system health, pipeline execution, and data processing are observed and managed.

---

#### Scope

Monitoring covers:

- Azure Data Factory pipelines  
- Azure Databricks jobs  
- infrastructure components  
- system logs and diagnostics  

---

#### Monitoring Components

The platform uses:

- Azure Log Analytics  
- Azure Data Factory monitoring  
- Azure Databricks logs  

---

#### Logic App Monitoring

The Logic App responsible for SharePoint ingestion includes built-in monitoring and alerting mechanisms.

This includes:

- validation of file names  
- segregation of invalid files into _rejected folder  
- email notifications for rejected files  

---

#### Role in Monitoring Strategy

The Logic App acts as the first validation layer in the data ingestion process.

    SharePoint
        │
        ▼
    Logic App (validation + alerting)
        │
        ▼
    RAW Layer

This ensures:

- early detection of data issues  
- prevention of invalid data entering the pipeline  
- immediate notification of ingestion problems  

---

#### Monitoring Scope

- ingestion success/failure  
- rejected files tracking  
- alert notifications  

---

#### Notes

- Logic App monitoring complements ADF and Databricks monitoring  
- focuses on ingestion validation rather than processing  

---

#### Architecture

    Data Factory
        │
        ▼
    Databricks
        │
        ▼
    Log Analytics

---

#### Objectives

- ensure pipeline reliability  
- detect failures quickly  
- enable debugging and root cause analysis  
- support operational visibility  

---

#### Design Principles

- centralized monitoring  
- clear error visibility  
- minimal complexity in dev  
- scalable for production  

---

#### Notes

- monitoring is currently focused on development visibility  
- alerting will be introduced in future stages  