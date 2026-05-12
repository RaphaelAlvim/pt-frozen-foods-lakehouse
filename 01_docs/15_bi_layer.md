# BI Layer — PT Frozen Foods

## Overview

This document describes the Business Intelligence (BI) layer implemented in the PT Frozen Foods data platform.

The BI layer represents the analytical consumption layer of the platform and was designed to transform the processed analytical datasets into business-oriented dashboards and executive-level visualizations.

The current implementation uses:

- Power BI Desktop
- Azure Databricks connector
- Import Mode
- analytics datasets built on the Gold layer

---

## BI Layer Purpose

The BI layer was introduced to:

- validate analytical consumption scenarios  
- demonstrate end-to-end platform usability  
- expose business insights through dashboards  
- support executive and operational analysis  
- validate the effectiveness of the Analytics layer  
- prepare the platform for future ML-driven insights  

The BI implementation is considered a continuation of the Analytics layer and not an isolated reporting artifact.

---

## Architecture Positioning

The BI layer consumes data from:

RAW → Bronze → Silver → Gold → Analytics → BI

The current dashboard consumes:

- `gold.analytics_sales_overview`

through:

- Azure Databricks connector
- Power BI Desktop

---

## Current BI Stack

### Visualization Tool

- Power BI Desktop

### Data Source

- Azure Databricks

### Authentication

- Personal Access Token (PAT)

### Connection Mode

- Import Mode

### Storage Format

- PBIP (Power BI Project)

---

## Connection Strategy

The dashboard connects directly to an existing Databricks compute resource.

Important decisions:

- SQL Warehouse was intentionally not used at this stage  
- existing Databricks compute was reused for simplicity and quota limitations  
- Import Mode was selected to simplify development and guarantee responsiveness during dashboard exploration  

Connection parameters used:

- Server Hostname  
- HTTP Path  
- Personal Access Token  

These parameters are retrieved from:

Compute → Advanced Options → JDBC/ODBC

---

## Dashboard Scope

The current implementation includes four analytical pages:

### 1. Executive Sales Overview

Purpose:

- high-level executive monitoring  
- KPI tracking  
- sales trend analysis  
- quick business overview  

Includes:

- Gross Sales  
- Net Sales  
- Margin  
- Margin %  
- Gross Sales Over Time  
- Top Products  
- Channel Performance  
- Salesperson Performance  

---

### 2. Revenue & Product Analytics

Purpose:

- product-level analytical exploration  
- revenue concentration analysis  
- category and brand performance evaluation  

Includes:

- Top Products by Gross Sales  
- Gross Sales by Category  
- Gross Sales by Brand  

---

### 3. Customer Analytics

Purpose:

- customer segmentation analysis  
- geographical distribution analysis  
- customer portfolio exploration  

Includes:

- Top Customers by Gross Sales  
- Gross Sales by Customer City  
- Revenue Distribution by Segment  
- Revenue Distribution by Commercial Cluster  

---

### 4. Salesperson Analytics

Purpose:

- commercial performance analysis  
- sales channel analysis by salesperson  
- product distribution analysis by salesperson  

Includes:

- Sales Performance Over Time  
- Sales Channel Distribution by Salesperson  
- Top Products by Salesperson  

---

## Analytics Dataset Consumption

The BI layer currently consumes:

- `analytics_sales_overview`

This dataset was intentionally created as a reusable analytical consumption structure.

Important characteristics:

- optimized for BI consumption  
- aggregated analytical structure  
- reduced dependency on direct fact table consumption  
- optimized with Delta + Liquid Clustering  

---

## Analytical Grain Evolution

During BI development, a new analytical requirement emerged:

- salesperson-level analysis

To support this requirement:

- `vendedor_id` was added to the Analytics layer grain

Original grain:

- `data_pedido`
- `cliente_id`
- `produto_id`
- `canal_id`

Updated grain:

- `data_pedido`
- `cliente_id`
- `produto_id`
- `canal_id`
- `vendedor_id`

Important:

- no metric logic was changed  
- no aggregation behavior was modified  
- all totals were validated before deployment  

This evolution was treated as a controlled analytical enhancement driven by BI consumption requirements.

---

## DAX Measures

The current dashboard includes basic DAX measures:

### Gross Sales

SUM(gross_sales_amount)

### Net Sales

SUM(net_sales_amount)

### Margin

SUM(gross_margin_amount)

### Margin %

DIVIDE([Margin], [Net Sales])

---

## PBIP Structure

The dashboard was saved using the PBIP format.

Advantages:

- Git-friendly structure  
- semantic model separation  
- report separation  
- easier version control  
- improved collaboration capability  

Current structure:

power_bi/

├── pt_frozen_foods_dashboard.pbip  
├── pt_frozen_foods_dashboard.Report/  
├── pt_frozen_foods_dashboard.SemanticModel/  
├── screenshots/  
├── exports/  
└── README.md  

---

## Git Versioning Strategy

The BI layer is fully versioned in Git.

Important rules:

- PBIP artifacts are versioned  
- semantic definitions are versioned  
- report definitions are versioned  
- screenshots and exports may be versioned when relevant  

Ignored artifacts:

- local cache files  
- `.pbi/` local settings  
- temporary semantic cache files  

Examples:

- `cache.abf`
- `localSettings.json`
- `editorSettings.json`

---

## Dashboard Design Principles

The dashboard implementation follows these principles:

- executive-oriented visualization  
- minimal visual clutter  
- consistent page structure  
- reusable visual patterns  
- domain-oriented analytical separation  
- business storytelling focus  
- analytical clarity over visual complexity  

---

## Visual Standardization

The dashboard pages were standardized to maintain:

- layout consistency  
- slicer positioning consistency  
- visual hierarchy consistency  
- analytical navigation consistency  

A common header structure was adopted across pages:

- page title  
- synchronized time slicer  
- aligned visual distribution  

---

## Time Filtering Strategy

The temporal slicer was synchronized conceptually across dashboard pages to maintain analytical consistency.

Current temporal analysis is primarily based on:

- Year-Month aggregation

instead of:

- daily granularity

This approach improves:

- trend readability  
- seasonality analysis  
- executive consumption quality  

---

## Data Privacy Considerations

Due to privacy and confidentiality restrictions:

- all dashboard visuals use synthetic data  
- no real company information is exposed  
- no real customer information is exposed  

The project focuses on:

- architecture  
- engineering practices  
- analytical design  
- platform organization  
- BI structure  

rather than real operational data.

---

## Current BI Maturity

The current BI implementation already demonstrates:

- analytical consumption layer design  
- semantic modeling concepts  
- executive dashboard organization  
- domain-oriented analytical pages  
- integration between Engineering and BI  
- reusable analytics dataset strategy  

The implementation is intentionally focused on:

- analytical consistency  
- engineering alignment  
- business readability  

instead of advanced visual effects or heavy UI customization.

---

## Future BI Improvements

Possible future enhancements:

- Power BI Service deployment  
- scheduled refresh configuration  
- Row-Level Security (RLS)  
- DirectQuery evaluation  
- additional analytical pages  
- advanced DAX measures  
- drill-through pages  
- tooltip pages  
- ML prediction integration into dashboards  

---

## Relationship with Future ML Layer

The BI layer also serves as preparation for future Machine Learning initiatives.

The current analytical structures can later support:

- sales forecasting  
- customer segmentation  
- churn prediction  
- recommendation systems  

The BI dashboards are expected to evolve in the future to consume:

- ML predictions  
- scoring outputs  
- forecasting outputs  
- clustering outputs  

---

## Conclusion

The BI layer successfully validates the final analytical consumption stage of the PT Frozen Foods platform.

The project now demonstrates an end-to-end data workflow including:

- infrastructure provisioning  
- ingestion  
- orchestration  
- distributed processing  
- governance  
- dimensional modeling  
- analytics layer design  
- executive BI consumption  

This establishes the foundation for the next platform evolution phase:

- Machine Learning integration