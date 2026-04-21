### Parameters and Future Incremental Strategy

#### Purpose

This document describes the parameterization strategy and the future implementation of incremental data processing in the PT Frozen Foods data platform.

It defines how pipelines can be made dynamic and scalable.

---

#### Parameterization Strategy

Parameterization allows pipelines to be flexible and reusable.

Typical parameters include:

- environment (dev / prod)  
- catalog name  
- schema name  
- table name  
- file paths  
- execution dates  

---

#### Current Implementation

At the current stage:

- pipelines are mostly static  
- parameters are not heavily used  
- focus is on validating full data flow  

---

#### Benefits of Parameterization

- reusable pipelines  
- reduced code duplication  
- easier environment switching  
- improved maintainability  

---

#### Future Implementation

Pipelines can be enhanced with:

- dynamic notebook paths  
- dynamic table names  
- configurable execution layers  
- environment-based configurations  

---

#### Incremental Processing Strategy

Future implementation will include incremental data processing.

This means:

- processing only new or updated data  
- avoiding full reloads  
- improving performance and cost efficiency  

---

#### Incremental Techniques

Possible approaches:

- watermark (last processed timestamp)  
- file-based ingestion (new files only)  
- Change Data Feed (Delta Lake)  
- partition-based processing  

---

#### Integration with ADF

ADF can support incremental processing by:

- passing parameters to notebooks  
- controlling execution windows  
- triggering partial pipeline execution  

---

#### Integration with Databricks

Databricks notebooks can implement incremental logic using:

- filtering by timestamp  
- Delta Lake merge operations  
- Auto Loader incremental ingestion  

---

#### Example Concept

    Last Processed Date → Parameter
             │
             ▼
    Filter New Data Only
             │
             ▼
    Process Incrementally
             │
             ▼
    Update Target Table

---

#### Design Principles

- start simple (full load)  
- evolve to incremental processing  
- maintain flexibility through parameters  
- align with performance and cost optimization  

---

#### Notes

- current pipelines use full load approach  
- incremental strategy is planned for future evolution  
- parameterization will be introduced progressively  