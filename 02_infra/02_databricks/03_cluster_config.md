### Databricks Cluster Configuration — PT Frozen Foods

#### Overview

This document describes the compute configuration used in the PT Frozen Foods data platform.

Databricks clusters are used as the execution engine for data processing pipelines orchestrated by Azure Data Factory (ADF).

---

#### Current Cluster Configuration

| Property | Value |
|----------|-------|
| Cluster Name | dbx-ptfrozenfoods-dev-bronze |
| Runtime | Databricks Runtime 14.3 LTS |
| Spark Version | Apache Spark 3.5.0 |
| Node Type | Standard_D4ds_v4 (16 GB, 4 cores) |
| Mode | Single Node |
| Access Mode | Standard |
| Photon | Enabled |
| Auto Termination | 15 minutes |
| Policy | Unrestricted |

---

#### Tags

| Key | Value |
|-----|------|
| project_name | pt_frozen_foods_251201 |
| layer | bronze |
| env | dev |

---

#### Cluster Strategy

At the current stage:

- A **single-node cluster** is used
- Designed for:
  - development
  - ADF pipeline execution
- Optimized for:
  - low cost
  - simplicity
  - controlled workloads

This configuration is sufficient for the current scale of the project.

---

#### Key Technical Considerations

##### Execution Environment Consistency

The cluster used by ADF is the **reference execution environment**.

Important observation:

- Differences were identified between:
  - serverless execution
  - cluster execution

Example:
- `try_to_date` worked in serverless
- failed in cluster (ADF execution)

Conclusion:
- All notebooks must be validated against this cluster
- Serverless is not used as a reference environment

---

##### Unity Catalog Integration

- Cluster operates with Unity Catalog enabled
- All data access is governed via:
  - catalog
  - schemas
  - external locations

---

##### Performance Configuration

- Photon enabled for query acceleration
- Delta Lake used as storage format
- Auto Optimize enabled
- Auto Compaction enabled
- Liquid Clustering applied in Gold layer

---

#### Workload Usage

The cluster is used for:

- Bronze ingestion (Auto Loader)
- Silver transformations and integration
- Gold modeling (dimensions, facts, marts)
- ADF pipeline execution

---

#### Integration with ADF

- ADF uses this cluster via Linked Service
- Notebooks are executed through Databricks Notebook Activities
- Execution depends on:
  - cluster availability
  - Unity Catalog permissions

---

#### Governance

- Access controlled via Unity Catalog
- Storage access via External Locations
- Permissions granted to:
  - user
  - ADF service principal

---

#### Future Evolution

- Introduce Job Clusters for production workloads
- Separate environments (DEV / TEST / PROD)
- Scale compute based on workload growth

---

#### Notes

- Current configuration prioritizes simplicity and cost-efficiency
- Suitable for development and controlled execution scenarios
- Not intended for high-scale production workloads

---

#### Conclusion

The Databricks cluster is the core execution layer of the platform.

It is tightly integrated with ADF and Unity Catalog and represents the authoritative runtime environment for all data processing workloads.