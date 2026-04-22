### Silver Layer

#### Purpose

This layer is responsible for cleaning, standardizing, and integrating data.

---

#### Structure

    exploratory/
    processing/

Processing is further organized by domain:

    crm/
    erp/
    reference/
    weather_api/
    web/
    integration/

---

#### Processing Logic

- data cleansing and validation  
- standardization of formats  
- enrichment and integration across datasets  

---

#### Integration

- combines data from multiple domains  
- creates consistent analytical datasets  

---

#### Characteristics

- improved data quality  
- standardized schemas  
- integrated datasets  

---

#### Naming Convention

    silver_<source>_<entity>

Example:

    silver_crm_clients  

---

#### Notes

- exploratory notebooks support validation  
- processing notebooks are used in production pipelines  