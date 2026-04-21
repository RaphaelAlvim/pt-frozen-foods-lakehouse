### Pipeline Design

#### Purpose

This document describes the design principles and architectural decisions used to build pipelines in the PT Frozen Foods data platform.

It defines how pipelines are structured, organized, and scaled.

---

#### Design Approach

The platform follows a modular pipeline architecture.

Each pipeline is responsible for a specific layer or processing stage.

This ensures:

- clear separation of responsibilities  
- easier maintenance  
- scalability  

---

#### Pipeline Structure

The design is based on two levels:

Master Pipeline

- pl_ptfrozenfoods_master  
- orchestrates the full execution flow  

Sub-Pipelines

- pl_bronze_ingestion  
- pl_silver_standardization  
- pl_silver_integration  
- pl_gold_dimensions  
- pl_gold_fact  
- pl_gold_marts  

---

#### Orchestration Strategy

- centralized orchestration via master pipeline  
- sequential execution across layers  
- dependency enforcement between stages  
- fail-fast behavior  

---

#### Layer-Based Design

Each pipeline is aligned with the Medallion Architecture:

- Bronze → ingestion  
- Silver → standardization and integration  
- Gold → business-ready datasets  

This ensures consistency between data processing and orchestration.

---

#### Responsibility Separation

- ADF → orchestration  
- Databricks → data processing  
- Unity Catalog → governance  
- Data Lake → storage  

---

#### Execution Model

- pipelines are triggered via master pipeline  
- sub-pipelines are not executed independently in production  
- execution flow is deterministic and controlled  

---

#### Scalability Considerations

The design allows future evolution:

- parallel execution of independent pipelines  
- domain-based pipeline separation  
- incremental processing  
- parameterized pipelines  

---

#### Integration with Repository

- all pipelines are versioned in Git  
- changes are managed through collaboration branch  
- deployment uses publish branch (adf_publish)  

---

#### Design Principles

- modularity  
- separation of concerns  
- simplicity in initial implementation  
- scalability for future enhancements  
- alignment with enterprise data platform patterns  

---

#### Notes

- current design prioritizes clarity over optimization  
- suitable for development and portfolio demonstration  
- ready for future extensions (BI and ML)  