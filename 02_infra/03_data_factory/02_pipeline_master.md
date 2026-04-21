### Pipeline Master

#### Purpose

This document describes the master orchestration pipeline of the PT Frozen Foods data platform.

The master pipeline is responsible for coordinating the execution of all data processing layers in the correct order.

---

#### Pipeline Name

pl_ptfrozenfoods_master

---

#### Role in Architecture

The master pipeline acts as the central orchestrator of the platform.

It ensures that all data processing steps are executed sequentially and successfully.

---

#### Execution Flow

The pipeline executes the following stages:

1. Bronze ingestion  
2. Silver standardization  
3. Silver integration  
4. Gold dimensions  
5. Gold fact  
6. Gold marts  

Each stage is executed via dedicated sub-pipelines.

---

#### Sub-Pipelines

- pl_bronze_ingestion  
- pl_silver_standardization  
- pl_silver_integration  
- pl_gold_dimensions  
- pl_gold_fact  
- pl_gold_marts  

---

#### Orchestration Logic

- sequential execution  
- dependency control between layers  
- failure propagation (pipeline stops on error)  
- full pipeline visibility through ADF  

---

#### Integration with Databricks

Each sub-pipeline executes Databricks notebooks via:

- Linked Service: ls_databricks_ptfrozenfoods_dev  
- Notebook path: Git-integrated workspace  
- Execution using job compute  

---

#### Design Principles

- modular pipeline architecture  
- clear separation of layers  
- single responsibility per pipeline  
- centralized orchestration  
- scalability for future extensions (BI and ML)  

---

#### Notes

- the master pipeline was successfully executed end-to-end  
- all layers (bronze, silver, gold) are validated  
- serves as the main entry point for platform execution  