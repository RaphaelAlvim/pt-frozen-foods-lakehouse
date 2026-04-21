### ADF Monitoring

#### Purpose

This document describes how Azure Data Factory pipelines are monitored in the PT Frozen Foods platform.

---

#### Monitoring Interface

ADF provides built-in monitoring through:

- pipeline runs  
- activity runs  
- execution history  

---

#### What is Monitored

- pipeline execution status  
- activity success and failure  
- execution duration  
- error messages  

---

#### Failure Visibility

When a pipeline fails:

- execution stops  
- error is logged in ADF  
- failure is visible in monitoring UI  

---

#### Debugging

Issues can be analyzed using:

- activity output logs  
- pipeline run details  
- error messages  

---

#### Integration with Databricks

- ADF captures Databricks execution status  
- errors are propagated from notebooks  
- debugging may require checking Databricks logs  

---

#### Future Enhancements

- automated alerts  
- integration with Log Analytics  
- pipeline health dashboards  

---

#### Notes

- monitoring is currently manual  
- suitable for development phase  
