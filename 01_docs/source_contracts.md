# Source Contracts — PT Frozen Foods

## Overview

This document defines the expected source contracts for all datasets entering the PT Frozen Foods platform.

A source contract describes how each dataset is expected to arrive in the platform, including:

- source domain
- dataset name
- ingestion mode
- file format
- RAW landing path
- operational notes

These contracts create a stable foundation for downstream Bronze processing and future pipeline automation.

---

## Contract Structure

Each dataset is defined by:

- source domain
- dataset name
- ingestion mode
- expected file format
- RAW landing path
- description
- notes

---

## CRM Sources

### Dataset: crm_clients

- source domain: crm
- dataset name: crm_clients
- ingestion mode: manual upload
- expected file format: csv
- RAW landing path: `raw/crm/crm_clients/load_date=YYYY-MM-DD/`
- description: customer master dataset
- notes: base customer dataset used for customer-related analysis and future dimensional modeling

### Dataset: crm_segmentacao

- source domain: crm
- dataset name: crm_segmentacao
- ingestion mode: manual upload
- expected file format: csv
- RAW landing path: `raw/crm/crm_segmentacao/load_date=YYYY-MM-DD/`
- description: customer segmentation information
- notes: supports analytical customer grouping and enrichment

### Dataset: crm_status

- source domain: crm
- dataset name: crm_status
- ingestion mode: manual upload
- expected file format: csv
- RAW landing path: `raw/crm/crm_status/load_date=YYYY-MM-DD/`
- description: customer status information
- notes: used for customer lifecycle interpretation and downstream enrichment

---

## ERP Sources

### Dataset: erp_fornecedores

- source domain: erp
- dataset name: erp_fornecedores
- ingestion mode: manual upload
- expected file format: csv
- RAW landing path: `raw/erp/erp_fornecedores/load_date=YYYY-MM-DD/`
- description: supplier dataset
- notes: supports supplier dimension and product-supplier relationships

### Dataset: erp_itens_pedido

- source domain: erp
- dataset name: erp_itens_pedido
- ingestion mode: manual upload
- expected file format: csv
- RAW landing path: `raw/erp/erp_itens_pedido/load_date=YYYY-MM-DD/`
- description: order line items dataset
- notes: supports transactional sales analysis at detailed granularity

### Dataset: erp_pedidos

- source domain: erp
- dataset name: erp_pedidos
- ingestion mode: manual upload
- expected file format: csv
- RAW landing path: `raw/erp/erp_pedidos/load_date=YYYY-MM-DD/`
- description: order header dataset
- notes: core sales transaction source

### Dataset: erp_produtos

- source domain: erp
- dataset name: erp_produtos
- ingestion mode: manual upload
- expected file format: csv
- RAW landing path: `raw/erp/erp_produtos/load_date=YYYY-MM-DD/`
- description: product master dataset
- notes: supports product dimension and product-related analytics

### Dataset: erp_vendedores

- source domain: erp
- dataset name: erp_vendedores
- ingestion mode: manual upload
- expected file format: csv
- RAW landing path: `raw/erp/erp_vendedores/load_date=YYYY-MM-DD/`
- description: sales representative dataset
- notes: supports commercial performance analysis by seller

---

## Reference Sources

### Dataset: reference_calendario

- source domain: reference
- dataset name: reference_calendario
- ingestion mode: sharepoint_logic_app
- expected file format: csv
- RAW landing path: `raw/reference/reference_calendario/load_date=YYYY-MM-DD/`
- description: calendar reference dataset
- notes: suitable for business-managed file updates and automated SharePoint ingestion

### Dataset: reference_canais_venda

- source domain: reference
- dataset name: reference_canais_venda
- ingestion mode: sharepoint_logic_app
- expected file format: csv
- RAW landing path: `raw/reference/reference_canais_venda/load_date=YYYY-MM-DD/`
- description: sales channel reference dataset
- notes: suitable for business-maintained channel mappings and controlled automated ingestion

### Dataset: reference_localidades

- source domain: reference
- dataset name: reference_localidades
- ingestion mode: sharepoint_logic_app
- expected file format: csv
- RAW landing path: `raw/reference/reference_localidades/load_date=YYYY-MM-DD/`
- description: locations reference dataset
- notes: suitable for controlled file-based ingestion from SharePoint

---

## Weather Source

### Dataset: weather_porto_daily

- source domain: weather_api
- dataset name: weather_porto_daily
- ingestion mode: manual upload
- expected file format: csv
- RAW landing path: `raw/weather_api/weather_porto_daily/load_date=YYYY-MM-DD/`
- description: daily weather dataset for Porto
- notes: supports weather-related business analysis and future forecasting features

---

## Web Source

### Dataset: web_event_logs

- source domain: web
- dataset name: web_event_logs
- ingestion mode: manual upload
- expected file format: csv
- RAW landing path: `raw/web/web_event_logs/load_date=YYYY-MM-DD/`
- description: web interaction event log dataset
- notes: supports digital behavior analysis, funnel analysis, and recommendation preparation

---

## Ingestion Mode Definitions

### manual upload

Used when the source is represented by synthetic datasets under controlled loading into RAW.

### sharepoint_logic_app

Used when the file is intended to arrive through SharePoint-triggered Azure Logic Apps into the RAW layer.

---

## Contract Stability

These source contracts should remain stable as the platform evolves.

Future enhancements may improve automation, metadata capture, or validation logic, but the source contract itself should remain the reference point for how each dataset is expected to arrive in the platform.