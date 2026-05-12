# Power BI Dashboard — PT Frozen Foods

## Overview

This folder contains the Power BI implementation of the PT Frozen Foods analytical consumption layer.

The dashboard was developed to demonstrate how the analytics datasets produced in the Lakehouse architecture can be transformed into business-oriented insights and executive dashboards.

The implementation uses:

- Power BI Desktop
- Azure Databricks connector
- Import Mode
- PBIP project structure

---

## Current Dashboard Pages

### 1. Executive Sales Overview

Executive monitoring dashboard including:

- Gross Sales
- Net Sales
- Margin
- Margin %
- sales trends
- channel performance
- salesperson performance

---

### 2. Revenue & Product Analytics

Product-focused analytical dashboard including:

- Top Products by Gross Sales
- Gross Sales by Category
- Gross Sales by Brand

---

### 3. Customer Analytics

Customer-focused analytical dashboard including:

- Top Customers by Gross Sales
- Gross Sales by Customer City
- Revenue Distribution by Segment
- Revenue Distribution by Commercial Cluster

---

### 4. Salesperson Analytics

Sales-focused analytical dashboard including:

- Sales Performance Over Time
- Sales Channel Distribution by Salesperson
- Top Products by Salesperson

---

## Project Structure

power_bi/

├── pt_frozen_foods_dashboard.pbip  
├── pt_frozen_foods_dashboard.Report/  
├── pt_frozen_foods_dashboard.SemanticModel/  
├── screenshots/  
├── exports/  
└── README.md  

---

## Data Source

Current analytical source:

- `gold.analytics_sales_overview`

Connection method:

- Azure Databricks connector
- Import Mode

---

## Screenshots

Dashboard screenshots are stored in:

screenshots/

---

## Data Privacy

All dashboard visuals use synthetic data generated exclusively for study and portfolio purposes.

No real company or customer information is exposed.

---

## Additional Documentation

For full architectural and BI layer documentation, refer to:

`01_docs/15_bi_layer.md`