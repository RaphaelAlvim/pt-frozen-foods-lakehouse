### Gold Layer

#### Purpose

This layer provides business-ready data for analytics and reporting.

---

#### Structure

    exploratory/
        dimensions/
        facts/
        marts/

    processing/
        dimensions/
        facts/
        marts/

---

#### Data Model

The Gold layer follows a dimensional model:

- dimensions  
- fact table  
- marts  

---

#### Processing Logic

- creation of dimensions  
- creation of fact table  
- generation of business marts  

---

#### Characteristics

- analytical data model  
- optimized for BI and reporting  
- enriched datasets  

---

#### Naming Convention

Dimensions:

    dim_<entity>

Facts:

    fact_<entity>

Marts:

    mart_<domain>

---

#### Notes

- exploratory notebooks validate business logic  
- processing notebooks build production datasets  
- this layer is the main consumption layer  