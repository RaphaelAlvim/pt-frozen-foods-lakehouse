### Dependencies and Execution Flow

#### Purpose

This document describes the dependencies and execution flow of pipelines in the PT Frozen Foods data platform.

It defines how data moves across layers and how pipelines depend on each other.

---

#### High-Level Flow

The platform follows a strict sequential execution model aligned with the Medallion Architecture:

Bronze → Silver → Gold

---

#### Execution Order

The pipelines are executed in the following order:

1. pl_bronze_ingestion  
2. pl_silver_standardization  
3. pl_silver_integration  
4. pl_gold_dimensions  
5. pl_gold_fact  
6. pl_gold_marts  

This order is enforced by the master pipeline.

---

#### Dependency Logic

Each layer depends on the successful completion of the previous one:

- Silver depends on Bronze  
- Gold depends on Silver  
- Marts depend on Facts and Dimensions  

If any step fails, the execution is stopped.

---

#### Pipeline Dependencies

    pl_ptfrozenfoods_master
            │
            ▼
    pl_bronze_ingestion
            │
            ▼
    pl_silver_standardization
            │
            ▼
    pl_silver_integration
            │
            ▼
    pl_gold_dimensions
            │
            ▼
    pl_gold_fact
            │
            ▼
    pl_gold_marts

---

#### Data Flow

    Raw Data
       │
       ▼
    Bronze (ingestion)
       │
       ▼
    Silver (standardization)
       │
       ▼
    Silver (integration)
       │
       ▼
    Gold (dimensions + facts)
       │
       ▼
    Gold (marts)
       │
       ▼
    Consumption (BI / ML)

---

#### Failure Handling

- execution stops on failure  
- downstream pipelines are not triggered  
- errors are visible in ADF monitoring  

---

#### Orchestration Behavior

- fully controlled by master pipeline  
- sequential execution  
- explicit dependency management  
- no parallel execution at current stage  

---

#### Future Evolution

The execution model can evolve to:

- partial parallelization (e.g., dimensions in parallel)  
- independent domain pipelines  
- event-driven execution  
- integration with ML pipelines  

---

#### Notes

- execution flow has been validated end-to-end  
- pipeline dependencies reflect real production patterns  
- structure is scalable and ready for extension  