# Notebook Strategy — PT Frozen Foods

## Overview

This document defines how notebooks are used in the PT Frozen Foods platform.

The objective is to clearly separate exploratory work from operational transformation logic, while keeping development practical, versioned, and aligned with the architecture of the project.

The platform uses Azure Databricks as the primary execution environment for data processing, while GitHub is used for version control and long-term code management.

---

## General Principle

Not all notebooks serve the same purpose.

In this project, notebooks are divided into two main categories:

1. exploratory notebooks
2. operational notebooks

This separation is important to avoid mixing data discovery activities with repeatable transformation logic.

---

## Exploratory Notebooks

Exploratory notebooks are used to understand datasets before defining the final transformation logic.

Their purpose is to:

- inspect source files
- understand schema and structure
- validate data quality
- test parsing logic
- analyze field behavior
- experiment with technical transformation ideas

These notebooks are expected to be more flexible and iterative.

They are not intended to be directly orchestrated by Azure Data Factory.

### Typical examples

- initial reading of RAW files
- profiling and inspection of source fields
- testing date parsing
- validating delimiters, nulls, and data types
- checking data volume and anomalies

### Recommended location

`04_notebooks/05_analytics/`

---

## Operational Notebooks

Operational notebooks are used to execute standardized and repeatable transformations in the platform.

Their purpose is to:

- read data from the expected source path
- apply defined transformation logic
- write structured outputs to the next layer
- support orchestration by Azure Data Factory

These notebooks must be cleaner, more stable, and more predictable than exploratory notebooks.

They should reflect logic that has already been validated.

### Typical examples

- RAW to Bronze transformations
- Bronze to Silver transformations
- Silver to Gold transformations

### Recommended locations

- `04_notebooks/02_bronze/`
- `04_notebooks/03_silver/`
- `04_notebooks/04_gold/`

---

## Execution Environment

Azure Databricks is the primary environment used to:

- access the Data Lake
- explore datasets
- test parsing logic
- execute operational transformations

This means exploratory work is normally performed directly in Databricks, where the data is already available and Spark can be used natively.

---

## Role of GitHub and VS Code

GitHub is the version control system of the project.

VS Code is the preferred local development environment for organizing and maintaining project artifacts outside the Databricks execution context.

In practice:

- Databricks is used to explore and validate transformations with real platform data
- GitHub stores the notebooks and documentation as versioned artifacts
- VS Code is used to organize, review, and maintain project code and structure

The objective is not to force all development to happen locally, but to ensure that what becomes part of the platform is eventually versioned and documented.

---

## Recommended Workflow

The recommended notebook workflow is:

1. explore the dataset in Azure Databricks
2. validate parsing and technical logic
3. define the intended Bronze, Silver, or Gold behavior
4. consolidate the logic into a cleaner operational notebook
5. version the notebook in GitHub
6. use Azure Data Factory to orchestrate the operational notebook when needed

This approach balances agility with maintainability.

---

## Relationship with ADF

Azure Data Factory is responsible for orchestration, not exploration.

This means:

- exploratory notebooks are used during design and validation
- operational notebooks are the ones expected to be triggered by ADF

ADF should only call notebooks that are stable, intentional, and properly aligned with the transformation layer they belong to.

---

## Layer Alignment

The notebook strategy follows the same layer philosophy of the platform:

### RAW
No notebook should transform and persist business-ready data directly from RAW to Gold.

### Bronze
Bronze notebooks perform the first technical transformation after ingestion.

Typical Bronze responsibilities include:

- reading RAW files
- handling parsing
- applying initial schema logic
- converting files into Delta format
- adding technical metadata

### Silver
Silver notebooks perform cleaning, integration, and enrichment.

Typical Silver responsibilities include:

- joining related datasets
- handling duplicates
- cleaning fields
- applying business-aware validation
- producing reusable curated datasets

### Gold
Gold notebooks create analytical delivery outputs.

Typical Gold responsibilities include:

- aggregations
- business-level analytical tables
- outputs for dashboards
- outputs for machine learning preparation

---

## Versioning Principle

All notebooks that become part of the platform logic should eventually be versioned.

This applies especially to:

- Bronze notebooks
- Silver notebooks
- Gold notebooks
- reusable exploration notebooks that document important design logic

Temporary or disposable exploration notebooks do not need to be preserved indefinitely, but any notebook that influences the final architecture or transformation rules should be committed to the repository.

---

## Practical Rule

A simple practical rule for this project is:

- use Databricks to discover and validate
- use GitHub to preserve and version
- use ADF to orchestrate what has already been defined

This keeps the project realistic, organized, and aligned with enterprise data engineering practices.