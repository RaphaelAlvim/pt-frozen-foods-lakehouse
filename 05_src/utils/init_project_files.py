from pathlib import Path
from textwrap import dedent

# ============================================================
# CONFIGURAÇÃO
# ============================================================

BASE_DIR = Path(r"C:\Users\rapha\Desktop\007 IT\001_DS_PROJECTS\pt_frozen_foods_251201")

FILES_CONTENT = {
    "README.md": dedent("""
        # PT Frozen Foods — Lakehouse Data Platform

        Projeto de dados com foco em:

        - Data Engineering
        - Lakehouse Architecture
        - Azure Data Platform
        - Databricks
        - Analytics
        - Machine Learning

        ## Objetivo

        Construir uma plataforma moderna de dados, partindo de dados-fonte
        simulados até camadas Bronze, Silver e Gold, prontas para BI e ML.

        ## Contexto

        O projeto é tratado como um caso real de negócio e arquitetura.
        O nome "PT Frozen Foods" é utilizado como nome substituto por motivos
        de confidencialidade.

        Os dados publicados neste repositório são sintéticos, gerados para fins
        de portfólio e demonstração técnica, preservando sigilo e privacidade.
        Apesar disso, a modelagem, a lógica de negócio e a arquitetura seguem
        princípios realistas de implementação.

        ## Arquitetura alvo

        - Fonte: SharePoint / ficheiros simulados / APIs
        - Armazenamento: ADLS Gen2
        - Orquestração: Azure Data Factory
        - Transformação: Databricks
        - Formato: Delta Lake
        - Consumo: Power BI + ML

        ## Estrutura do projeto

        - 01_docs
        - 02_infra
        - 03_data
        - 04_notebooks
        - 05_src
        - 06_outputs
        - 07_tests

        ## Estado atual

        Estrutura inicial criada.
        Próximo passo: geração de dados-fonte e implementação das camadas do Lakehouse.
    """).strip() + "\n",

    "requirements.txt": dedent("""
        pandas>=2.0
        numpy>=1.24
        matplotlib>=3.7
        seaborn>=0.12
        jupyter>=1.0
        notebook>=7.0
        ipykernel>=6.0
        openpyxl>=3.1
        pyarrow>=14.0
        python-dotenv>=1.0
    """).strip() + "\n",

    ".gitignore": dedent("""
        # ============================================================
        # Python
        # ============================================================
        __pycache__/
        *.py[cod]
        *$py.class
        *.pyo
        *.pyd

        # Virtual environments
        .venv/
        venv/
        env/
        ENV/

        # ============================================================
        # Jupyter / Notebook
        # ============================================================
        .ipynb_checkpoints/

        # ============================================================
        # Environment / Secrets
        # ============================================================
        .env
        .env.*
        !.env.example

        # ============================================================
        # OS / Editor
        # ============================================================
        .DS_Store
        Thumbs.db
        desktop.ini
        .vscode/
        .idea/

        # ============================================================
        # Logs / Temp
        # ============================================================
        *.log
        logs/
        tmp/
        temp/

        # ============================================================
        # Terraform
        # ============================================================
        .terraform/
        *.tfstate
        *.tfstate.*
        crash.log
        override.tf
        override.tf.json
        *_override.tf
        *_override.tf.json
        .terraform.lock.hcl

        # ============================================================
        # Outputs / Reports generated locally
        # ============================================================
        06_outputs/profiling/*
        06_outputs/reports/*
        06_outputs/samples/*
        03_data/exports/*

        # Keep folders tracked
        !06_outputs/profiling/.gitkeep
        !06_outputs/reports/.gitkeep
        !06_outputs/samples/.gitkeep
        !03_data/exports/.gitkeep

        # ============================================================
        # Large / local data (adjust if needed)
        # ============================================================
        # Uncomment below if you do NOT want local raw/bronze/silver/gold data in git
        # 03_data/raw/**
        # 03_data/bronze/**
        # 03_data/silver/**
        # 03_data/gold/**
        # !03_data/raw/**/.gitkeep
        # !03_data/bronze/**/.gitkeep
        # !03_data/silver/**/.gitkeep
        # !03_data/gold/**/.gitkeep
    """).strip() + "\n",

    ".env.example": dedent("""
        # Azure / Databricks / Storage
        AZURE_SUBSCRIPTION_ID=
        AZURE_TENANT_ID=
        AZURE_CLIENT_ID=
        AZURE_CLIENT_SECRET=

        DATABRICKS_HOST=
        DATABRICKS_TOKEN=

        STORAGE_ACCOUNT_NAME=
        CONTAINER_NAME=
    """).strip() + "\n",

    "01_docs/project_overview.md": dedent("""
        # Project Overview

        ## Project name
        PT Frozen Foods — Lakehouse Data Platform

        ## Purpose
        Build an end-to-end data platform with realistic source data,
        Bronze / Silver / Gold transformations, analytics and ML-ready datasets.

        ## Context
        This project is documented and structured as a real-world data initiative.
        "PT Frozen Foods" is a substitute name used for confidentiality reasons.

        The datasets published in this repository are synthetic and intended for
        portfolio and technical demonstration purposes. The business logic,
        architecture and implementation approach are designed to remain realistic.

        ## Main goals
        - Simulate realistic business sources
        - Implement a Lakehouse architecture
        - Deliver business-oriented Gold tables
        - Support analytics, forecasting, recommendation and churn

        ## Current phase
        Initial project setup
    """).strip() + "\n",

    "01_docs/business_context.md": dedent("""
        # Business Context

        This project represents a real business-style data platform scenario,
        documented with a substitute company name for confidentiality.

        PT Frozen Foods is used as a public-facing name for portfolio publication.

        The business operates with multiple commercial channels, including:
        - E-commerce
        - Field Sales
        - Phone Orders
        - Marketplace

        The project aims to support decisions related to:
        - product performance
        - customer behavior
        - channel evolution
        - seasonality
        - climate impact
        - digital interaction analysis
    """).strip() + "\n",

    "01_docs/architecture_overview.md": dedent("""
        # Architecture Overview

        ## Target architecture
        Lakehouse

        ## Main components
        - SharePoint / source files / APIs
        - Azure Data Lake Storage Gen2
        - Azure Data Factory
        - Databricks
        - Delta Lake
        - Power BI
        - Machine Learning

        ## Data flow
        Sources -> Raw -> Bronze -> Silver -> Gold -> BI / ML

        ## Publication note
        The architecture reflects a real-world implementation pattern.
        Source names and published datasets were adapted for confidentiality
        and portfolio use.
    """).strip() + "\n",

    "01_docs/data_model.md": dedent("""
        # Data Model

        ## Source domains
        - ERP
        - CRM
        - Web
        - Reference
        - Weather API

        ## Main analytical targets
        - Sales performance
        - Product performance
        - Customer performance
        - Channel performance
        - Customer 360
        - Recommendation dataset
        - Forecast dataset
        - Churn dataset

        ## Note
        The published data model is built from synthetic source data for portfolio
        purposes, while preserving a realistic analytical structure.
    """).strip() + "\n",

    "01_docs/roadmap.md": dedent("""
        # Roadmap

        ## Phase 1
        - Create repository structure
        - Create initial project files
        - Build synthetic data generator

        ## Phase 2
        - Generate raw source datasets
        - Implement Bronze layer
        - Implement Silver layer

        ## Phase 3
        - Implement Gold layer
        - Build analytics notebooks
        - Build Customer 360

        ## Phase 4
        - Build ML datasets
        - Forecast
        - Recommendation
        - Churn

        ## Phase 5
        - Terraform infrastructure
        - ADF orchestration
        - Databricks execution
    """).strip() + "\n",

    "01_docs/decisions_log.md": dedent("""
        # Decisions Log

        ## Initial decisions
        - Use Lakehouse architecture
        - Use Azure as target cloud
        - Use ADLS + Databricks + ADF
        - Use Delta Lake as target table format
        - Organize repository in enterprise-style structure
        - Use synthetic published data while preserving realistic business logic
        - Treat the project as a real implementation case with anonymized naming
    """).strip() + "\n",

    "01_docs/project_history.md": dedent("""
        # Project History

        This file will store the chronological evolution of the project.

        Suggested use:
        - record relevant decisions
        - record architecture changes
        - record new datasets and transformations
        - support future README writing

        Note:
        The repository uses a substitute company name and synthetic datasets for
        publication, while preserving the technical history and design decisions
        of a real-world project structure.
    """).strip() + "\n",

    "02_infra/adf/pipeline_design.md": dedent("""
        # ADF Pipeline Design

        This document will describe:
        - source ingestion logic
        - dataset design
        - trigger strategy
        - orchestration flow
        - dependency management
    """).strip() + "\n",

    "02_infra/adf/datasets.md": dedent("""
        # ADF Datasets

        This document will list and describe the datasets used in Azure Data Factory.
    """).strip() + "\n",

    "02_infra/adf/linked_services.md": dedent("""
        # ADF Linked Services

        This document will describe the linked services used by ADF
        (SharePoint, ADLS, Databricks, etc.).
    """).strip() + "\n",

    "02_infra/adf/orchestration_notes.md": dedent("""
        # Orchestration Notes

        This document will register orchestration design decisions
        and future ADF implementation notes.
    """).strip() + "\n",

    "02_infra/databricks/cluster_config.md": dedent("""
        # Databricks Cluster Config

        This document will describe:
        - cluster type
        - runtime version
        - autoscaling strategy
        - access mode
    """).strip() + "\n",

    "02_infra/databricks/job_design.md": dedent("""
        # Databricks Job Design

        This document will describe the execution strategy for:
        - Bronze jobs
        - Silver jobs
        - Gold jobs
        - ML jobs
    """).strip() + "\n",

    "02_infra/databricks/workflows.md": dedent("""
        # Databricks Workflows

        This document will describe workflow orchestration
        and job dependency structure inside Databricks.
    """).strip() + "\n",

    "02_infra/databricks/delta_conventions.md": dedent("""
        # Delta Conventions

        This document will define conventions for:
        - table naming
        - partition strategy
        - merge strategy
        - update / overwrite rules
    """).strip() + "\n",

    "02_infra/cicd/deployment_strategy.md": dedent("""
        # Deployment Strategy

        This document will describe:
        - environment strategy (dev / prod)
        - git flow
        - release approach
        - CI/CD for Terraform, ADF and Databricks
    """).strip() + "\n",

    "02_infra/cicd/release_notes.md": dedent("""
        # Release Notes

        This file will register relevant infrastructure and deployment releases.
    """).strip() + "\n",

    "02_infra/sql/external_tables.sql": dedent("""
        -- External tables definitions
        -- To be implemented later
    """).strip() + "\n",

    "02_infra/sql/gold_views.sql": dedent("""
        -- Gold serving views
        -- To be implemented later
    """).strip() + "\n",

    "02_infra/sql/validation_queries.sql": dedent("""
        -- Validation queries
        -- To be implemented later
    """).strip() + "\n",

    "07_tests/test_generation.py": dedent("""
        def test_placeholder():
            assert True
    """).strip() + "\n",

    "07_tests/test_transformations.py": dedent("""
        def test_placeholder():
            assert True
    """).strip() + "\n",

    "07_tests/test_validations.py": dedent("""
        def test_placeholder():
            assert True
    """).strip() + "\n",

    "07_tests/test_gold_outputs.py": dedent("""
        def test_placeholder():
            assert True
    """).strip() + "\n",
}

GITKEEP_PATHS = [
    "03_data/raw/erp/.gitkeep",
    "03_data/raw/crm/.gitkeep",
    "03_data/raw/web/.gitkeep",
    "03_data/raw/reference/.gitkeep",
    "03_data/raw/weather_api/.gitkeep",
    "03_data/bronze/.gitkeep",
    "03_data/silver/.gitkeep",
    "03_data/gold/.gitkeep",
    "03_data/exports/.gitkeep",
    "06_outputs/figures/.gitkeep",
    "06_outputs/tables/.gitkeep",
    "06_outputs/profiling/.gitkeep",
    "06_outputs/samples/.gitkeep",
    "06_outputs/reports/.gitkeep",
]


# ============================================================
# FUNÇÕES
# ============================================================

def write_file_if_empty_or_missing(file_path: Path, content: str) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)

    if not file_path.exists():
        file_path.write_text(content, encoding="utf-8")
        print(f"[WRITE] {file_path}")
        return

    existing = file_path.read_text(encoding="utf-8").strip()
    if existing == "":
        file_path.write_text(content, encoding="utf-8")
        print(f"[FILL ] {file_path}")
    else:
        print(f"[SKIP ] {file_path} já tem conteúdo")


def create_gitkeep(file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    if not file_path.exists():
        file_path.touch()
        print(f"[KEEP ] {file_path}")
    else:
        print(f"[SKIP ] {file_path} já existe")


# ============================================================
# MAIN
# ============================================================

def main():
    if not BASE_DIR.exists():
        raise FileNotFoundError(f"A pasta base não existe: {BASE_DIR}")

    print("=" * 70)
    print("Preenchendo ficheiros iniciais do projeto")
    print(f"Base: {BASE_DIR}")
    print("=" * 70)

    for rel_path, content in FILES_CONTENT.items():
        write_file_if_empty_or_missing(BASE_DIR / rel_path, content)

    print("-" * 70)

    for rel_path in GITKEEP_PATHS:
        create_gitkeep(BASE_DIR / rel_path)

    print("=" * 70)
    print("Ficheiros iniciais preparados com sucesso.")
    print("=" * 70)


if __name__ == "__main__":
    main()