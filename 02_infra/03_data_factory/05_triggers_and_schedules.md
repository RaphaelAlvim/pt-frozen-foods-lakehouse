### Triggers and Schedules

#### Purpose

This document describes the triggering and scheduling strategy used in Azure Data Factory (ADF) for the PT Frozen Foods data platform.

It defines how and when pipelines are executed.

---

#### Trigger Types

The platform supports the following trigger types:

- manual trigger  
- scheduled trigger  
- event-based trigger (future use)  

---

#### Current Implementation

At the current stage of the project, execution is primarily:

- manual via ADF interface  
- manual via pipeline testing  
- used for development and validation  

No automated scheduling is active yet.

---

#### Planned Scheduling Strategy

In a production scenario, pipelines would be executed using scheduled triggers.

Typical configuration:

- daily execution  
- execution during off-peak hours  
- dependency on data availability  

Example:

- master pipeline runs once per day  
- full refresh of Bronze → Silver → Gold  

---

#### Trigger Scope

Triggers are associated with:

- pl_ptfrozenfoods_master (primary trigger)  

Sub-pipelines are not triggered directly.

---

#### Execution Flow with Trigger

    Trigger
       │
       ▼
    pl_ptfrozenfoods_master
       │
       ▼
    Full pipeline execution (Bronze → Silver → Gold)

---

#### Future Enhancements

The triggering model can evolve to:

- event-based ingestion (e.g., new files in Data Lake)  
- partial pipeline execution  
- domain-specific triggers  
- integration with external systems  

---

#### Integration with BI and ML

Triggers can be extended to:

- refresh Power BI datasets after pipeline execution  
- trigger ML training pipelines  
- schedule model scoring workflows  

ADF acts as the orchestration layer for these processes.

---

#### Design Principles

- centralized triggering via master pipeline  
- simplicity in development phase  
- scalability for production scenarios  
- alignment with enterprise orchestration patterns  

---

#### Notes

- current execution is manual for development control  
- scheduling will be introduced in production scenarios  
- triggers are designed to be simple and extensible  