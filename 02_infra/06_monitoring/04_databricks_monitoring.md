### Databricks Monitoring

#### Purpose

This document describes how Azure Databricks workloads are monitored in the PT Frozen Foods platform.

---

#### Monitoring Scope

Monitoring includes:

- notebook execution  
- job runs  
- cluster behavior  
- logs and errors  

---

#### Available Tools

- job run logs  
- notebook output  
- cluster logs  

---

#### What is Monitored

- execution success/failure  
- runtime duration  
- error messages  
- resource usage  

---

#### Debugging

Errors are analyzed using:

- notebook output  
- stack traces  
- cluster logs  

---

#### Integration with ADF

- ADF triggers notebook execution  
- failures are returned to ADF  
- root cause is typically found in Databricks  

---

#### Future Enhancements

- integration with Log Analytics  
- centralized logging  
- performance monitoring dashboards  

---

#### Notes

- monitoring is currently focused on development  
- logs are sufficient for debugging and validation  
