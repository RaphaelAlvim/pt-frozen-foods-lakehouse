### Data Model

#### Purpose

This document describes the high-level analytical data model used in the PT Frozen Foods data platform.

It provides an overview of the main datasets available in the Gold layer, their business role, and how they relate to each other.

---

#### Modeling Approach

The Gold layer follows a dimensional modeling approach.

It is organized around:

- dimensions  
- fact tables  
- marts  

This structure supports analytical workloads, business intelligence, and future machine learning use cases.

---

#### Gold Layer Structure

The main analytical objects are:

Dimensions

- dim_calendar  
- dim_customer  
- dim_product  
- dim_sales_channel  
- dim_supplier  
- dim_weather  

Fact

- fact_sales  

Marts

- mart_customer_product_mix  
- mart_customer_sales  
- mart_sales_performance  

---

#### Dimensional Model

The analytical core of the platform is centered on `fact_sales`.

This fact table is enriched with descriptive business attributes and supports the creation of marts without requiring excessive joins.

This design improves:

- query simplicity  
- analytical usability  
- processing efficiency  

---

#### Dimension Tables

##### dim_calendar

Provides time-based attributes for analysis.

Typical usage:

- year, quarter, month, day  
- weekday analysis  
- period comparisons  
- trend and seasonality analysis  

---

##### dim_customer

Provides customer-related descriptive attributes.

Typical usage:

- customer segmentation  
- geography-based analysis  
- commercial cluster analysis  
- customer status and classification  

---

##### dim_product

Provides product-related descriptive attributes.

Typical usage:

- category analysis  
- brand analysis  
- product status  
- product performance by segment  

---

##### dim_sales_channel

Provides information about the commercial channel.

Typical usage:

- sales channel analysis  
- channel comparison  
- performance by acquisition or sales route  

---

##### dim_supplier

Provides supplier reference data.

Typical usage:

- supplier analysis  
- product-to-supplier relationship  
- procurement-related business views  

---

##### dim_weather

Provides weather-related contextual attributes.

Typical usage:

- weather impact analysis  
- correlation between weather and sales behavior  
- environmental enrichment of business events  

---

#### Fact Table

##### fact_sales

Represents the central business event of the analytical model.

Business meaning:

- sales activity at transactional analytical level  

Role in the model:

- central fact table connecting calendar, customer, product, channel, supplier, and weather context  
- primary source for business marts  

Important design characteristic:

- this is an enriched fact table  
- it already includes descriptive business attributes from multiple analytical dimensions  

This means that many analyses can be performed directly from `fact_sales` without additional joins.

Typical analytical usage:

- sales trends  
- customer behavior  
- product performance  
- channel performance  
- contextual business analysis  

---

#### Data Marts

##### mart_customer_product_mix

Business-oriented mart focused on the relationship between customers and product mix.

Typical usage:

- customer purchase composition  
- category mix analysis by customer  
- product concentration and diversity by customer  

---

##### mart_customer_sales

Business-oriented mart focused on customer-level sales performance.

Typical usage:

- customer ranking  
- revenue by customer  
- customer contribution analysis  
- performance by customer segment  

---

##### mart_sales_performance

Business-oriented mart focused on overall commercial performance.

Typical usage:

- revenue and volume analysis  
- performance by time, channel, or category  
- business KPI tracking  
- executive reporting  

---

#### Relationship Logic

The logical model can be interpreted as:

    dim_calendar
         │
    dim_customer
         │
    dim_product
         │
    dim_sales_channel
         │
    dim_supplier
         │
    dim_weather
         │
         ▼
      fact_sales
         │
         ├── mart_customer_product_mix
         ├── mart_customer_sales
         └── mart_sales_performance

In practice, `fact_sales` acts as the main analytical base, while marts provide more direct business-facing views.

---

#### Grain Considerations

At this stage, grain is documented at a conceptual level.

The expected analytical interpretation is:

- dimensions → one row per business entity  
- fact_sales → one row per sales event or sales line at analytical transaction level  
- marts → one row per business aggregation level defined by the mart purpose  

Detailed physical grain validation can be documented later if needed.

---

#### Business Value

This model supports:

- descriptive analytics  
- performance analysis  
- business segmentation  
- reporting and dashboarding  
- future feature generation for ML workloads  

---

#### Design Principles

- dimensional modeling for analytical usability  
- enriched fact design for simpler consumption  
- marts derived from reusable analytical structures  
- business-oriented semantics in the Gold layer  
- scalability for future BI and ML use cases  

---

#### Notes

- this document provides a high-level model view  
- it is based on the implemented Gold layer structure and notebook design  
- detailed column-level documentation can be added later if required  