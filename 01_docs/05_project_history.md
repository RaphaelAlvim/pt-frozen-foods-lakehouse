# Project History — PT Frozen Foods

## Overview

This document describes the evolution of the PT Frozen Foods data platform.

The project started as an analytical initiative and progressively evolved into a structured, cloud-based data platform aligned with enterprise practices.

---

## Initial Stage

The project began with a focus on understanding the business and its data.

Key activities included:

- dataset preparation
- exploratory data analysis
- identification of key business use cases

This stage established the analytical foundation of the project.

---

## Analytical Development

The project evolved into deeper analytical work, including:

- data validation and profiling
- sales and customer behavior analysis
- web interaction analysis
- definition of initial analytical outputs

This phase helped shape the future Gold layer and business-oriented datasets.

---

## Architectural Evolution

As complexity increased, the project transitioned into a full data platform design.

Key developments:

- definition of a Lakehouse architecture
- adoption of Azure as the cloud platform
- introduction of Infrastructure as Code (Terraform)
- separation of ingestion, processing, and orchestration layers

---

## Infrastructure Foundation

The platform infrastructure was provisioned using Terraform, including:

- Resource Group
- ADLS Gen2 with RAW, Bronze, Silver, Gold layers
- Azure Data Factory
- Azure Databricks workspace
- Azure Key Vault
- Log Analytics

This established a scalable and reproducible foundation.

---

## Ingestion Strategy

A hybrid ingestion model was implemented:

- Azure Logic Apps for automated ingestion (SharePoint reference data)
- manual ingestion for synthetic datasets representing enterprise systems

This approach balances realism with confidentiality constraints.

---

## Current State

The project is now structured as an enterprise-grade data platform with:

- defined architecture and data layers
- operational ingestion processes
- standardized documentation
- alignment with real-world data engineering practices

---

## Next Steps

The next phase focuses on:

- Bronze layer implementation in Databricks
- Silver and Gold transformations
- orchestration with Data Factory
- data quality and validation
- advanced analytics and machine learning use cases
