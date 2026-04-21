### Log Analytics

#### Purpose

This document describes the use of Azure Log Analytics in the PT Frozen Foods data platform.

---

#### Role in Architecture

Log Analytics provides centralized logging and monitoring.

It collects:

- pipeline execution logs  
- system diagnostics  
- operational metrics  

---

#### Integration

Log Analytics is connected to:

- Azure Data Factory  
- Azure Databricks (future integration)  

---

#### Capabilities

- log querying using KQL  
- centralized log storage  
- monitoring dashboards  
- integration with alerts  

---

#### Future Enhancements

- configure alerts for pipeline failures  
- integrate Databricks logs  
- build monitoring dashboards  

---

#### Notes

- currently provisioned via Terraform  
- used as the foundation for monitoring strategy  