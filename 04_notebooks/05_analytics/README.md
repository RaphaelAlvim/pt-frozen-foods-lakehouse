### Analytics Layer

#### Purpose

This layer contains analytical datasets and notebooks designed to enable efficient, scalable, and business-oriented data consumption.

It bridges the gap between raw analytical structures (Gold layer) and business-facing use cases such as BI, reporting, and advanced analysis.

---

#### Scope

- exploratory analysis to evaluate analytical capabilities  
- definition of analytical data structures  
- implementation of optimized analytics datasets  
- support for BI and business reporting  

---

#### Structure

- `exploratory/`  
  Used to analyze existing datasets (fact + marts), validate business questions, and define new analytical structures  

- `processing/`  
  Contains production-ready notebooks responsible for materializing analytics datasets  

---

#### Current Implementation

- `analytics_sales_overview`  
  - grain: `data_pedido + cliente_id + produto_id + canal_id`  
  - source: `gold.fact_sales`  
  - purpose: enable multi-dimensional analysis across customer, product, channel, and time  
  - optimized for analytical consumption using Delta + Liquid Clustering  

---

#### Characteristics

- designed for analytical consumption (BI, dashboards, ad-hoc queries)  
- optimized for performance and cost efficiency  
- reduces dependency on the fact table for common analytical use cases  
- built based on validated business requirements  

---

#### Design Approach

- analytics datasets are created only when justified by real analytical needs  
- avoids unnecessary duplication of marts or pre-aggregations  
- focuses on balancing flexibility and performance  
- prioritizes reusable and scalable data structures  

---

#### Notes

- the current implementation includes a single analytics dataset  
- additional datasets will only be introduced if required by recurring use cases or performance constraints  