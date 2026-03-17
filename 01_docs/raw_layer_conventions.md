# RAW Layer Conventions — PT Frozen Foods

## Overview

This document defines the organizational conventions for the RAW layer in ADLS Gen2.

The RAW layer is the landing zone of the platform. Its purpose is to receive incoming files exactly as they arrive from source systems or controlled ingestion processes, without applying business transformations.

The goal of these conventions is to ensure consistency, traceability, and readiness for downstream Bronze processing.

---

## Purpose of the RAW Layer

The RAW layer is responsible for:

- receiving incoming files from all ingestion mechanisms
- preserving original file structure and content
- separating ingestion from transformation logic
- enabling repeatable and auditable downstream processing
- supporting manual and automated landing patterns

No business transformation should happen in the RAW layer.

---

## Directory Structure

Files in the RAW layer must be organized using the following hierarchy:

source_domain  
-> dataset_name  
-> load_date=YYYY-MM-DD  

This means the directory structure follows three logical levels:

1. source domain  
2. dataset  
3. load date partition  

---

## Standard RAW Structure

The standard structure is:

raw/
  crm/
    crm_clients/
      load_date=YYYY-MM-DD/
    crm_segmentacao/
      load_date=YYYY-MM-DD/
    crm_status/
      load_date=YYYY-MM-DD/

  erp/
    erp_fornecedores/
      load_date=YYYY-MM-DD/
    erp_itens_pedido/
      load_date=YYYY-MM-DD/
    erp_pedidos/
      load_date=YYYY-MM-DD/
    erp_produtos/
      load_date=YYYY-MM-DD/
    erp_vendedores/
      load_date=YYYY-MM-DD/

  reference/
    reference_calendario/
      load_date=YYYY-MM-DD/
    reference_canais_venda/
      load_date=YYYY-MM-DD/
    reference_localidades/
      load_date=YYYY-MM-DD/

  weather_api/
    weather_porto_daily/
      load_date=YYYY-MM-DD/

  web/
    web_event_logs/
      load_date=YYYY-MM-DD/

---

## Naming Rules

The following naming rules apply:

- source domains must be lowercase
- dataset names must be lowercase
- words must be separated with underscores
- load date partitions must follow the format `load_date=YYYY-MM-DD`

Examples:

- `crm/crm_clients/load_date=2026-03-18/`
- `erp/erp_pedidos/load_date=2026-03-18/`
- `reference/reference_calendario/load_date=2026-03-18/`

---

## File Naming Guidance

Inside each partition folder, files should preserve a clear and source-aligned name.

Examples:

- `crm_clients.csv`
- `erp_pedidos.csv`
- `reference_calendario.csv`
- `weather_porto_daily.csv`
- `web_event_logs.json`

If multiple arrivals occur on the same date, the file name may optionally include a timestamp.

Examples:

- `crm_clients_20260318_101500.csv`
- `reference_localidades_20260318_153000.csv`

---

## Supported Ingestion Modes

These RAW conventions must be used consistently for both ingestion modes:

### Manual ingestion
Used for:
- CRM datasets
- ERP datasets
- weather data
- web event data

### Logic App ingestion
Used for:
- reference_calendario
- reference_canais_venda
- reference_localidades

The landing path standard remains the same regardless of how the file arrives.

---

## Why These Conventions Matter

These conventions are important because they:

- improve organization and discoverability
- simplify Databricks ingestion logic
- make pipeline behavior more predictable
- support repeatable Bronze processing
- reduce ambiguity between datasets and ingestion runs
- preserve traceability of data arrivals

---

## Downstream Impact

The Bronze layer will depend directly on this RAW organization.

Databricks notebooks and future orchestration logic will use these folder patterns to:

- identify the source dataset
- detect new arrivals
- process data partition by partition
- maintain structured ingestion workflows

For this reason, RAW conventions must remain stable once defined.