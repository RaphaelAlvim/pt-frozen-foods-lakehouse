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