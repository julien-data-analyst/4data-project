# src/dagster_and_dbt/defs/project.py
from pathlib import Path

from dagster_dbt import DbtProject

# Indiquer le chemin pour retrouver le fichier manifest.json à partir de l'emplacement du fichier
dbt_project = DbtProject(
  project_dir=Path(__file__).joinpath("../../../../..", "dbt").resolve(),
)

dbt_project.prepare_if_dev()
