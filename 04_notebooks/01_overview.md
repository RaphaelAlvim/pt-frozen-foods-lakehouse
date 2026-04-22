### Notebooks Overview

#### Purpose

This document describes the notebook structure used in the PT Frozen Foods data platform.

It explains how data processing is organized across layers and how notebooks are structured for development and production.

---

#### Structure

The notebooks are organized by layer:

- 01_source  
- 02_bronze  
- 03_silver  
- 04_gold  
- 05_analytics (future)  
- 06_ml (future)  

---

#### Notebook Types

Two main types of notebooks are used:

Exploratory

- used for analysis and validation  
- supports development decisions  
- not used in production pipelines  

Processing

- used for production data pipelines  
- executed by Azure Data Factory  
- optimized for performance and reliability  

---

#### Execution Flow

The notebooks follow the Medallion Architecture:

    Bronze → Silver → Gold

Each layer depends on the previous one.

---

#### Design Principles

- separation between exploratory and processing logic  
- modular notebook design  
- alignment with data layers  
- reproducibility and scalability  

---

#### Notes

- notebooks are versioned in Git  
- processing notebooks are designed for production execution  
- exploratory notebooks support validation and business understanding  