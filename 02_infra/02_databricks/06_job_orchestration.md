# Job Orchestration

## Overview

This document describes the job orchestration strategy adopted in the **PT Frozen Foods** project. The orchestration layer ensures reliable, scalable, and automated execution of data pipelines within the Azure Lakehouse architecture.

The solution integrates Azure Data Factory, Azure Databricks, and Azure Logic Apps to enable secure, efficient, and enterprise-grade data processing workflows for Business Intelligence and Machine Learning.

---

## Orchestration Architecture

The PT Frozen Foods platform leverages Azure-native services to coordinate data ingestion, transformation, and delivery processes.

```text
SharePoint / External Sources
            │
            ▼
        Azure Logic Apps
            │
            ▼
      Azure Data Factory
            │
            ▼
      Azure Databricks
            │
            ▼
 Azure Data Lake Storage Gen2
            │
            ▼
     Power BI & Machine Learning
```

---

## Orchestration Components

| Component | Service | Purpose |
|-----------|---------|---------|
| Workflow Automation | Azure Logic Apps | Automates data ingestion from SharePoint |
| Pipeline Orchestration | Azure Data Factory | Coordinates and schedules data pipelines |
| Data Processing | Azure Databricks | Executes notebooks for Bronze, Silver, and Gold layers |
| Data Storage | Azure Data Lake Storage Gen2 | Stores data following the Medallion Architecture |
| Data Governance | Unity Catalog | Manages access control and metadata |
| Monitoring and Observability | Azure Log Analytics | Tracks pipeline and job performance |
| Analytics and Reporting | Power BI | Consumes curated data for visualization |
| Machine Learning | Azure Databricks / ML Frameworks | Supports predictive analytics and model development |

---

## Azure Data Factory Pipelines

Azure Data Factory (ADF) serves as the primary orchestration engine in enterprise environments.

### Key Responsibilities

- Scheduling and triggering data pipelines.
- Executing Azure Databricks notebooks.
- Managing dependencies between workflows.
- Orchestrating end-to-end data processing.
- Supporting incremental and batch ingestion.
- Integrating with Azure Logic Apps and external systems.

### Pipeline Activities

| Activity | Description |
|----------|-------------|
| Databricks Notebook | Executes data transformation notebooks |
| Lookup | Retrieves configuration and metadata |
| Copy Data | Moves data between supported sources |
| Web Activity | Integrates with external services |
| Trigger | Initiates scheduled or event-based workflows |

---

## Ingestion Strategy for Synthetic and Production Data

Due to confidentiality constraints, the PT Frozen Foods project utilizes synthetic datasets for demonstration purposes. As a result, Azure Data Factory (ADF) is not currently used for data ingestion within this development environment.

Instead, synthetic data is generated and ingested directly into the platform to simulate real-world business scenarios while ensuring data privacy and compliance.

In a real production environment, Azure Data Factory serves as the primary ingestion and orchestration tool for integrating enterprise data sources.

### Production Ingestion Sources

| Source System | Service | Purpose |
|---------------|---------|---------|
| ERP Systems | Azure Data Factory | Ingestion of orders, products, and suppliers |
| CRM Systems | Azure Data Factory | Ingestion of customer and segmentation data |
| Web Logs and APIs | Azure Data Factory | Integration of digital and operational data |
| Reference Data (SharePoint) | Azure Logic Apps + Azure Data Factory | Automated ingestion of business reference datasets |
| Weather APIs | Azure Data Factory | Integration of external environmental data |

This hybrid orchestration model ensures scalability, automation, and alignment with enterprise-grade data engineering best practices.

---

## Azure Databricks Job Execution

Azure Databricks performs data processing across the Medallion Architecture.

| Layer | Notebook Purpose |
|-------|------------------|
| Bronze | Data ingestion using Auto Loader |
| Silver | Data cleansing and standardization |
| Gold | Dimensional modeling and analytics-ready datasets |

### Notebook Organization

| Layer | Directory |
|-------|-----------|
| Bronze | `04_notebooks/02_bronze/` |
| Silver | `04_notebooks/03_silver/` |
| Gold | `04_notebooks/04_gold/` |

---

## Azure Logic Apps Integration

Azure Logic Apps enable automated ingestion of reference data and external sources.

### Use Case

| Source | Purpose |
|--------|---------|
| SharePoint | Automated ingestion of reference datasets |

Logic Apps trigger downstream processes orchestrated by Azure Data Factory.

---

## Triggering Mechanisms

| Trigger Type | Service | Description |
|--------------|---------|-------------|
| Scheduled | Azure Data Factory | Executes pipelines at defined intervals |
| Event-Based | Azure Logic Apps | Initiates workflows upon external events |
| Manual | Azure Data Factory | Supports on-demand execution |
| Development | Azure Databricks | Allows interactive execution during testing |

---

## Monitoring and Observability

Monitoring ensures reliability, traceability, and operational visibility.

| Service | Capability |
|---------|------------|
| Azure Data Factory | Pipeline monitoring and diagnostics |
| Azure Databricks | Job execution logs, analytics, and machine learning workloads |
| Azure Log Analytics | Centralized logging and observability |
| Unity Catalog | Data lineage and governance |

---

## Security and Governance

| Feature | Implementation |
|---------|----------------|
| Authentication | Managed Identity |
| Data Governance | Unity Catalog |
| Access Control | Role-Based Access Control (RBAC) |
| Secrets Management | Azure Key Vault |
| Secure Connectivity | Access Connector and Private Networking |
| Compliance | Microsoft Azure Security Best Practices |

---

## Integration with Infrastructure as Code

All orchestration components are provisioned and managed using Terraform.

| Component | Status |
|-----------|--------|
| Azure Data Factory | Managed via Terraform |
| Azure Databricks | Managed via Terraform |
| Azure Logic Apps | Managed via Terraform |
| Azure Key Vault | Managed via Terraform |
| Azure Storage | Managed via Terraform |
| Monitoring Resources | Managed via Terraform |

---

## Role in the PT Frozen Foods Architecture

The orchestration layer coordinates data movement and processing across the Lakehouse platform.

```text
Data Sources
      │
      ▼
Azure Logic Apps
      │
      ▼
Azure Data Factory
      │
      ▼
Azure Databricks
      │
      ▼
Azure Data Lake Storage Gen2
      │
      ▼
Power BI and Machine Learning
```

---

## Best Practices Implemented

- Modular and scalable pipeline design.
- Separation of orchestration and transformation logic.
- Automation using Azure-native services.
- Secure authentication via Managed Identity.
- Centralized governance through Unity Catalog.
- Infrastructure managed with Terraform (IaC).
- Incremental data processing using Auto Loader.
- Monitoring and observability through Azure Monitor.
- Alignment with the Medallion Architecture.
- Adoption of enterprise-grade security standards.

---

## Notes

- Synthetic data is used in this project for confidentiality and demonstration purposes.
- Sensitive information has been excluded or masked for security reasons.
- The orchestration framework follows Microsoft and Databricks best practices.
- The architecture is designed for scalability, reliability, and maintainability.
- Configurations may evolve as the platform scales.

---

## References

- https://learn.microsoft.com/azure/data-factory/
- https://learn.microsoft.com/azure/databricks/
- https://learn.microsoft.com/azure/logic-apps/
- https://learn.microsoft.com/azure/databricks/lakehouse/
- https://learn.microsoft.com/azure/architecture/
- https://learn.microsoft.com/azure/terraform/
