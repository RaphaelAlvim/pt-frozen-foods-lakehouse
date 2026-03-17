# RAW Implementation Plan — PT Frozen Foods

## Overview

This document defines the first practical implementation step for the ingestion layer of the PT Frozen Foods platform.

After defining the ingestion strategy, RAW conventions, and source contracts, the next implementation step is to create the physical RAW structure in ADLS Gen2.

This ensures that all future ingestion mechanisms — both manual uploads and SharePoint + Logic App flows — land data in a controlled and standardized location.

---

## Decision

The first practical implementation step for the ingestion layer is:

- create the physical RAW directory structure in ADLS Gen2

This step comes before Logic App implementation and before manual loading execution.

---

## Rationale

The RAW layer is the common landing zone for all source data entering the platform.

Creating the RAW structure first provides:

- a stable target for manual uploads
- a stable target for Logic App ingestion
- consistency with documented source contracts
- readiness for downstream Bronze processing
- reduced ambiguity during implementation

Without a defined physical landing structure, ingestion processes would be more error-prone and less reproducible.

---

## Scope

The initial implementation will focus on the following source domains:

- crm
- erp
- reference
- weather_api
- web

And the following datasets:

### CRM
- crm_clients
- crm_segmentacao
- crm_status

### ERP
- erp_fornecedores
- erp_itens_pedido
- erp_pedidos
- erp_produtos
- erp_vendedores

### Reference
- reference_calendario
- reference_canais_venda
- reference_localidades

### Weather
- weather_porto_daily

### Web
- web_event_logs

---

## Standard Structure

The physical structure must follow this pattern:

source_domain  
-> dataset_name  
-> load_date=YYYY-MM-DD  

Examples:

- raw/crm/crm_clients/load_date=YYYY-MM-DD/
- raw/erp/erp_pedidos/load_date=YYYY-MM-DD/
- raw/reference/reference_calendario/load_date=YYYY-MM-DD/
- raw/weather_api/weather_porto_daily/load_date=YYYY-MM-DD/
- raw/web/web_event_logs/load_date=YYYY-MM-DD/

---

## Implementation Sequence

The RAW implementation will follow this sequence:

1. create the base source-domain folders
2. create dataset-level folders
3. validate naming consistency
4. use the structure as the official landing target for future ingestion

---

## Relationship with Next Steps

After the RAW structure is physically created, the next steps will be:

1. define and execute the manual upload process
2. implement Logic App for SharePoint-based ingestion
3. validate landing behavior
4. move toward Bronze processing design