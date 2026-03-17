# Decisions Log — PT Frozen Foods

## Purpose

This document records key architectural and technical decisions made throughout the project.

---

## Decision 001 — Lakehouse Architecture

Adopt a Lakehouse architecture combining flexibility and analytical capability.

---

## Decision 002 — Synthetic Data Usage

Use synthetic datasets and a fictional company name due to confidentiality constraints.

---

## Decision 003 — Terraform (IaC)

Use Terraform for infrastructure provisioning to ensure reproducibility, modularity, and version control.

---

## Decision 004 — ADF as Orchestrator

Use Azure Data Factory primarily for orchestration, coordinating Databricks processing.

---

## Decision 005 — Hybrid Ingestion

Use Logic Apps and manual ingestion to represent realistic enterprise data flows.

---

## Decision 006 — Incremental Development

Build the platform progressively, starting from infrastructure to full data processing.