### Job Orchestration — PT Frozen Foods

#### Overview

This document describes the orchestration strategy used in the PT Frozen Foods data platform.

Azure Data Factory (ADF) is the central orchestration engine, responsible for executing Databricks notebooks in a controlled and sequential pipeline.

---

#### Orchestration Model

The platform follows a simple and structured orchestration flow:

ADF → Databricks → Unity Catalog → ADLS

- ADF orchestrates execution
- Databricks performs processing
- Unity Catalog governs access
- Data is stored in ADLS

---

#### ADF Pipeline Structure

A modular pipeline design is implemented.

##### Master Pipeline

- pl_ptfrozenfoods_master

Responsible for orchestrating the full execution flow.

##### Subpipelines

| Pipeline | Purpose |
|----------|--------|
| pl_bronze_ingestion | Data ingestion |
| pl_silver_standardization | Data standardization |
| pl_silver_integration | Data integration |
| pl_gold_dimensions | Dimension tables |
| pl_gold_fact | Fact table |
| pl_gold_marts | Analytical marts |

---

#### Execution Flow

The pipelines are executed sequentially:

1. Bronze ingestion
2. Silver standardization
3. Silver integration
4. Gold dimensions
5. Gold fact
6. Gold marts

Each step depends on the successful completion of the previous one.

---

#### Notebook Execution

ADF executes Databricks notebooks using:

- Databricks Notebook Activities
- Linked Service to Databricks

All notebooks are executed from:

/Workspace/Shared/pt-frozen-foods-lakehouse/...

This ensures alignment with the repository (source of truth).

---

#### Integration with Databricks

- ADF triggers notebook execution
- Execution runs on the configured cluster
- Access is controlled via Unity Catalog
- Permissions are granted to the ADF service principal

---

#### Triggering

Current execution modes:

- Manual (primary)
- Development/testing execution

Future:

- Scheduled triggers
- Event-based triggers (optional)

---

#### Monitoring

Monitoring is performed through:

- ADF pipeline execution logs
- Databricks notebook execution logs

Centralized monitoring via Log Analytics is not yet implemented.

---

#### Design Decisions

- Orchestration logic is separated from transformation logic
- ADF is used only for orchestration, not transformation
- Databricks is responsible for all data processing
- Pipelines are modular and reusable
- Execution is deterministic and sequential

---

#### Known Constraints

- Single cluster used for all workloads (DEV phase)
- No retry/timeout strategy implemented yet
- No CI/CD for pipelines at this stage

---

#### Future Evolution

- Add retry and failure handling
- Introduce scheduling
- Implement CI/CD for ADF pipelines
- Introduce environment separation (DEV / TEST / PROD)

---

#### Notes

- Synthetic data is used in the current environment
- ADF ingestion is simulated via controlled inputs
- Architecture is simplified for development purposes

---

#### Conclusion

ADF orchestrates the full data pipeline in a structured and controlled way, ensuring consistent execution across all layers of the platform.
