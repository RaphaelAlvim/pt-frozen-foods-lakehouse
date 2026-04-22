### Bronze Layer

#### Purpose

This layer is responsible for ingesting raw data into the platform.

---

#### Structure

    ingestion/
        crm/
        erp/
        reference/
        weather_api/
        web/

---

#### Processing Logic

- ingestion is performed using Auto Loader  
- raw files are read from ADLS  
- data is written as Delta tables  
- minimal transformation is applied  

---

#### Characteristics

- schema applied  
- raw structure preserved  
- incremental ingestion supported  

---

#### Naming Convention

    bronze_<source>_<entity>

Example:

    bronze_erp_orders  

---

#### Notes

- this layer serves as the entry point for structured data  
- designed for scalability and ingestion performance  