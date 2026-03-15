from pathlib import Path

# ============================================================
# CONFIGURAÇÃO
# ============================================================

BASE_DIR = Path(r"C:\Users\rapha\Desktop\007 IT\001_DS_PROJECTS\pt_frozen_foods_251201")

DIRECTORIES = [
    "01_docs",
    "02_infra/terraform/envs/dev",
    "02_infra/terraform/envs/prod",
    "02_infra/terraform/modules/resource_group",
    "02_infra/terraform/modules/storage_account",
    "02_infra/terraform/modules/databricks_workspace",
    "02_infra/terraform/modules/data_factory",
    "02_infra/terraform/modules/key_vault",
    "02_infra/terraform/modules/monitoring",
    "02_infra/sql",
    "02_infra/adf",
    "02_infra/databricks",
    "02_infra/cicd/github_actions",
    "02_infra/diagrams",
    "03_data/raw/erp",
    "03_data/raw/crm",
    "03_data/raw/web",
    "03_data/raw/reference",
    "03_data/raw/weather_api",
    "03_data/bronze",
    "03_data/silver",
    "03_data/gold",
    "03_data/exports",
    "04_notebooks/01_source",
    "04_notebooks/02_bronze",
    "04_notebooks/03_silver",
    "04_notebooks/04_gold",
    "04_notebooks/05_analytics",
    "04_notebooks/06_ml",
    "05_src/config",
    "05_src/data_generation",
    "05_src/ingestion",
    "05_src/transformations",
    "05_src/pipelines",
    "05_src/validations",
    "05_src/utils",
    "06_outputs/figures",
    "06_outputs/tables",
    "06_outputs/profiling",
    "06_outputs/samples",
    "06_outputs/reports",
    "07_tests",
]

FILES = [
    "README.md",
    "requirements.txt",
    ".gitignore",
    ".env.example",
    "01_docs/project_overview.md",
    "01_docs/business_context.md",
    "01_docs/architecture_overview.md",
    "01_docs/data_model.md",
    "01_docs/roadmap.md",
    "01_docs/decisions_log.md",
    "01_docs/project_history.md",
    "02_infra/terraform/main.tf",
    "02_infra/terraform/variables.tf",
    "02_infra/terraform/outputs.tf",
    "02_infra/terraform/providers.tf",
    "02_infra/terraform/terraform.tfvars.example",
    "02_infra/sql/external_tables.sql",
    "02_infra/sql/gold_views.sql",
    "02_infra/sql/validation_queries.sql",
    "02_infra/adf/pipeline_design.md",
    "02_infra/adf/datasets.md",
    "02_infra/adf/linked_services.md",
    "02_infra/adf/orchestration_notes.md",
    "02_infra/databricks/cluster_config.md",
    "02_infra/databricks/job_design.md",
    "02_infra/databricks/workflows.md",
    "02_infra/databricks/delta_conventions.md",
    "02_infra/cicd/deployment_strategy.md",
    "02_infra/cicd/release_notes.md",
    "07_tests/test_generation.py",
    "07_tests/test_transformations.py",
    "07_tests/test_validations.py",
    "07_tests/test_gold_outputs.py",
]

# ============================================================
# FUNÇÕES
# ============================================================

def create_directories(base_dir: Path, directories: list[str]) -> None:
    for rel_path in directories:
        dir_path = base_dir / rel_path
        dir_path.mkdir(parents=True, exist_ok=True)
        print(f"[DIR ] {dir_path}")


def create_files(base_dir: Path, files: list[str]) -> None:
    for rel_path in files:
        file_path = base_dir / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)

        if not file_path.exists():
            file_path.touch()
            print(f"[FILE] {file_path}")
        else:
            print(f"[SKIP] {file_path} já existe")


# ============================================================
# MAIN
# ============================================================

def main():
    if not BASE_DIR.exists():
        raise FileNotFoundError(f"A pasta base não existe: {BASE_DIR}")

    print("=" * 60)
    print("Criando estrutura do projeto PT Frozen Foods 251201")
    print(f"Base: {BASE_DIR}")
    print("=" * 60)

    create_directories(BASE_DIR, DIRECTORIES)
    create_files(BASE_DIR, FILES)

    print("=" * 60)
    print("Estrutura criada com sucesso.")
    print("=" * 60)


if __name__ == "__main__":
    main()