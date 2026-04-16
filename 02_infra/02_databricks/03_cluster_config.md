# Azure Databricks Cluster Configuration

## Overview

This document describes the cluster configuration for the **PT Frozen Foods** project. Azure Databricks compute resources are designed to support scalable, secure, and high-performance data processing workloads following Microsoft and Databricks best practices.

The configuration aligns with the Lakehouse architecture and supports data engineering, analytics, and machine learning workloads.

---

## Compute Configuration

| Property | Value |
|----------|-------|
| Workspace | ptfrozenfoods-dbx-dev |
| Compute Type | All-Purpose Compute / Job Compute |
| Access Mode | Standard |
| Unity Catalog Compatibility | Enabled |
| Databricks Runtime | Latest supported Long-Term Support (LTS) version |
| Photon Acceleration | Enabled |
| Autoscaling | Enabled |
| Auto Termination | Enabled |
| Cloud Provider | Microsoft Azure |
| Region | West Europe |
| Pricing Tier | Premium |

---

## Node Configuration

| Property | Value |
|----------|-------|
| Driver Node Type | To be defined based on workload requirements |
| Worker Node Type | To be defined based on workload requirements |
| Minimum Workers | To be defined |
| Maximum Workers | To be defined |
| Virtual Machine Provider | Azure Virtual Machines |

> **Note:** Node types and scaling parameters will be finalized based on performance and cost optimization requirements.

---

## Security Configuration

| Feature | Status |
|---------|--------|
| Unity Catalog Enabled | Yes |
| Managed Identity Authentication | Yes |
| Azure Databricks Access Connector | Enabled |
| Role-Based Access Control (RBAC) | Enabled |
| Secure Cluster Connectivity (No Public IP) | Enabled |
| Data Encryption at Rest | Enabled |
| Data Encryption in Transit | Enabled |

---

## Performance Optimization

The following optimizations are applied to ensure efficient data processing:

- **Photon Engine** for accelerated query execution.
- **Delta Lake** for reliable and performant data storage.
- **Auto Optimize** to improve file sizes during writes.
- **Auto Compaction** to mitigate the small files problem.
- **Liquid Clustering** for improved query performance.
- **Caching** for frequently accessed datasets.
- **Autoscaling** for dynamic workload adaptation.

---

## Supported Workloads

| Workload | Description |
|----------|-------------|
| Data Ingestion | Incremental ingestion using Auto Loader |
| Data Transformation | ETL/ELT pipelines across Bronze, Silver, and Gold layers |
| Data Modeling | Creation of fact and dimension tables |
| Data Quality | Validation and standardization processes |
| Analytics | Preparation of datasets for Power BI |
| Machine Learning | Model development and experimentation |

---

## Integration with Azure Services

| Service | Purpose |
|---------|---------|
| Azure Data Lake Storage Gen2 | Data storage |
| Azure Data Factory | Pipeline orchestration |
| Unity Catalog | Data governance and access control |
| Azure Key Vault | Secrets management |
| Azure Log Analytics | Monitoring and observability |
| Terraform | Infrastructure provisioning (IaC) |
| Power BI | Business intelligence and reporting |

---

## Role in the PT Frozen Foods Architecture

Azure Databricks clusters serve as the computational engine of the platform, enabling scalable and secure data processing within the Lakehouse architecture.

## Architectural Context

```
Azure Data Factory
        │
        ▼
Azure Databricks Clusters
        │
        ▼
Unity Catalog
        │
        ▼
Azure Data Lake Storage Gen2
        │
        ▼
Power BI
```

---

## Governance and Best Practices

- Use **Job Compute** for production workloads.
- Use **All-Purpose Compute** for development and testing.
- Enable **Photon Acceleration** for performance optimization.
- Enforce **Unity Catalog** for centralized governance.
- Apply the **Principle of Least Privilege** for access control.
- Enable **Auto Termination** to optimize costs.
- Use **Autoscaling** for workload elasticity.
- Monitor compute usage through Azure Log Analytics.
- Manage infrastructure using Terraform.

---

## Notes

- Configurations may evolve as the project scales.
- Sensitive information has been excluded for security purposes.
- All settings follow Microsoft Azure and Databricks best practices.
- Cluster-specific parameters will be updated once production compute is fully provisioned.

---

## References

- https://learn.microsoft.com/azure/databricks/compute/configure
- https://learn.microsoft.com/azure/databricks/clusters
- https://learn.microsoft.com/azure/databricks/runtime
- https://learn.microsoft.com/azure/databricks/photon
- https://learn.microsoft.com/azure/databricks/unity-catalog
- https://learn.microsoft.com/azure/databricks/lakehouse