# Ingestion Strategy — PT Frozen Foods

## Overview

This document defines how data enters the PT Frozen Foods platform.

The ingestion layer is designed to reflect a realistic enterprise architecture while operating under confidentiality constraints. Because the project uses synthetic datasets and a fictional company name, some data sources are manually loaded into the RAW zone, while selected sources are ingested through Azure Logic Apps integrated with SharePoint.

This approach preserves architectural realism while keeping the implementation controlled, reproducible, and compatible with Infrastructure as Code and future CI/CD practices.

---

## Ingestion Principles

The ingestion strategy follows these principles:

- keep the landing process simple and controlled
- separate ingestion from orchestration and processing
- ensure all data first lands in the RAW layer
- use automated ingestion only where it adds architectural value
- use manual ingestion where synthetic data and confidentiality constraints make direct source integration unnecessary

---

## Ingestion Modes

The platform uses two ingestion modes:

### 1. Manual upload to ADLS RAW

Manual upload is used for datasets that represent enterprise operational systems but are implemented with synthetic data.

This applies to:

- CRM datasets
- ERP datasets
- weather data
- web event data

The goal is to simulate the arrival of business data from systems such as CRM, ERP platforms, APIs, and operational logs without exposing or depending on real production systems.

### 2. SharePoint + Azure Logic Apps

Azure Logic Apps integrated with SharePoint is used for selected reference datasets.

This applies to:

- reference_calendar
- reference_sales_channels
- reference_locations

These datasets are suitable for SharePoint-driven ingestion because they are smaller, more stable, and closer to business-managed reference files.

---

## Source Classification

### Manual ingestion sources

#### CRM
- crm_clients
- crm_segmentacao
- crm_status

#### ERP
- erp_fornecedores
- erp_itens_pedido
- erp_pedidos
- erp_produtos
- erp_vendedores

#### Weather API
- weather_porto_daily

#### Web
- web_event_logs

### Logic App + SharePoint sources

#### Reference
- reference_calendario
- reference_canais_venda
- reference_localidades

---

## Role of the RAW Layer

All data must first land in the RAW layer of ADLS Gen2.

The RAW layer is the controlled landing zone of the platform and is responsible for:

- preserving the original structure of incoming files
- isolating ingestion from transformation
- enabling traceability of source arrivals
- supporting repeatable Bronze processing

No business transformations should occur in the RAW layer.

---

## Relationship with ADF and Databricks

Azure Data Factory is not the primary ingestion engine in this project.

ADF is used as the orchestration layer after data is already available in RAW.

Its role is to:

- trigger Databricks notebooks
- coordinate processing across Bronze, Silver, and Gold
- manage execution dependencies

Azure Databricks is responsible for transformation and processing after ingestion is complete.

---

## Why This Strategy Was Chosen

This ingestion strategy reflects a practical balance between realism and confidentiality.

The project is based on a real business scenario, but real production data cannot be used. As a result:

- manual ingestion is used to represent realistic enterprise sources with synthetic data
- Logic Apps is used where event-driven or business-managed ingestion adds architectural value
- ADF remains focused on orchestration rather than direct landing

This keeps the platform aligned with real-world data engineering practices while remaining safe for documentation and portfolio use.

---

## Future Evolution

This ingestion model can evolve in future iterations to include:

- more automated source integrations
- stronger event-driven ingestion patterns
- ingestion metadata tracking
- CI/CD integration for ingestion-related artifacts