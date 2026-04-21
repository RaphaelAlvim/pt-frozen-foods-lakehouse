### Deployment Strategy

#### Purpose

This document describes the deployment strategy used in the PT Frozen Foods data platform.

It defines how changes are managed, versioned, and promoted across environments.

---

#### Environment Strategy

The platform is currently deployed in a single environment:

- dev  

Future environments may include:

- qa  
- prod  

Each environment will have isolated infrastructure and configurations.

---

#### Git Strategy

The project uses Git as the source of truth.

- main → collaboration branch  
- adf_publish → ADF publish branch  

All changes are version-controlled and tracked through the repository.

---

#### Deployment Approach

Current approach:

- manual deployment  
- development directly in Databricks (Git folder)  
- ADF pipelines published via Azure Data Factory  

Future approach:

- automated deployments via CI/CD  
- environment-based promotion (dev → qa → prod)  

---

#### Component Deployment

Terraform

- provisions infrastructure  
- executed manually (future: CI/CD)  

ADF

- pipelines developed in UI  
- published to adf_publish branch  

Databricks

- notebooks stored in Git repository  
- executed via Git-integrated workspace  

Logic Apps

- workflow JSON stored in repository  
- deployed manually (future: Terraform or CI/CD)  

---

#### CI/CD Strategy (Future)

Planned implementation:

- GitHub Actions for automation  
- Terraform deployment pipelines  
- validation and testing steps  
- controlled promotion across environments  

---

#### Design Principles

- Git as single source of truth  
- separation between development and deployment  
- reproducible infrastructure  
- gradual evolution to automation  

---

#### Notes

- current setup is optimized for development and portfolio demonstration  
- CI/CD will be introduced as the platform evolves  