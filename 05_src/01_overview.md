### Source Code Layer

#### Purpose

This folder contains reusable Python code used in the PT Frozen Foods data platform.

It complements Databricks notebooks by providing modular, reusable, and scalable components.

---

#### Scope

The source layer supports:

- data generation  
- reusable transformations  
- validation logic  
- utility functions  
- future pipeline abstraction  

---

#### Structure

config/

- configuration files and global variables  

data_generation/

- synthetic data generation scripts  

ingestion/

- reusable ingestion logic (future)  

pipelines/

- pipeline orchestration logic (future)  

transformations/

- reusable transformation functions (future)  

utils/

- utility scripts for project setup and automation  

validations/

- data quality validation rules (future)  

---

#### Design Approach

- notebooks handle execution  
- src provides reusable logic  
- separation between orchestration and logic  

---

#### Current Status

- data generation implemented  
- utilities partially implemented  
- other modules reserved for future development  

---

#### Future Evolution

- centralize transformation logic  
- introduce validation framework  
- enable reuse across pipelines  
- support CI/CD integration  

---

#### Notes

- this layer is designed for scalability  
- avoids duplication of logic in notebooks  