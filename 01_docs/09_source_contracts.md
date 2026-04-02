# Source Contracts — PT Frozen Foods

## Overview

This document defines the expected source contracts for all datasets entering the PT Frozen Foods platform.

A source contract describes how each dataset must arrive in the platform, ensuring consistency and stability for downstream processing.

---

## Contract Structure

Each dataset is defined by:

- source domain
- dataset name
- ingestion mode
- file format
- RAW landing path
- description

---

## CRM Sources

### crm_clients
- domain: crm
- ingestion: manual_upload
- format: csv
- path: raw/crm/crm_clients/load_date=YYYY-MM-DD/
- description: customer master data

---

### crm_segmentation
- domain: crm
- ingestion: manual_upload
- format: csv
- path: raw/crm/crm_segmentation/load_date=YYYY-MM-DD/
- description: customer segmentation data

---

### crm_status
- domain: crm
- ingestion: manual_upload
- format: csv
- path: raw/crm/crm_status/load_date=YYYY-MM-DD/
- description: customer status data

---

## ERP Sources

### erp_orders
- domain: erp
- ingestion: manual_upload
- format: csv
- path: raw/erp/erp_orders/load_date=YYYY-MM-DD/
- description: order header data

---

### erp_order_items
- domain: erp
- ingestion: manual_upload
- format: csv
- path: raw/erp/erp_order_items/load_date=YYYY-MM-DD/
- description: order line items

---

### erp_products
- domain: erp
- ingestion: manual_upload
- format: csv
- path: raw/erp/erp_products/load_date=YYYY-MM-DD/
- description: product master data

---

### erp_salespersons
- domain: erp
- ingestion: manual_upload
- format: csv
- path: raw/erp/erp_salespersons/load_date=YYYY-MM-DD/
- description: sales representatives

---

### erp_suppliers
- domain: erp
- ingestion: manual_upload
- format: csv
- path: raw/erp/erp_suppliers/load_date=YYYY-MM-DD/
- description: supplier data

---

## Reference Sources

### reference_calendar
- domain: reference
- ingestion: sharepoint_logic_app
- format: csv
- path: raw/reference/reference_calendar/load_date=YYYY-MM-DD/
- description: calendar reference data

---

### reference_locations
- domain: reference
- ingestion: sharepoint_logic_app
- format: csv
- path: raw/reference/reference_locations/load_date=YYYY-MM-DD/
- description: location reference data

---

### reference_sales_channels
- domain: reference
- ingestion: sharepoint_logic_app
- format: csv
- path: raw/reference/reference_sales_channels/load_date=YYYY-MM-DD/
- description: sales channel reference data

---

## Weather Source

### weather_porto_daily
- domain: weather_api
- ingestion: manual_upload
- format: csv
- path: raw/weather_api/weather_porto_daily/load_date=YYYY-MM-DD/
- description: daily weather data

---

## Web Source

### web_event_logs
- domain: web
- ingestion: manual_upload
- format: csv
- path: raw/web/web_event_logs/load_date=YYYY-MM-DD/
- description: web interaction logs

---

## Ingestion Modes

### manual_upload
Used for controlled ingestion of synthetic datasets.

### sharepoint_logic_app
Used for automated ingestion via SharePoint and Logic App.

---

## Contract Stability

Source contracts must remain stable.

They define how data enters the platform and should not change unless there is a controlled evolution of the source system.