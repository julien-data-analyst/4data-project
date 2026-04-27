import pytest
import pandas as pd
from src.dagster_informatics_companies.defs.assets.companies import extract_load_companies
import dagster as dg
import os
from src.dagster_informatics_companies.defs.resources import PostgresResource
from src.dagster_informatics_companies.defs.partitions import categorie_partitions

# Ajouter jobs
def test_companies_jobs():
    """
    Vérifie que le job s'exécute correctement
    """
    result = dg.materialize(
        assets=[extract_load_companies],
        resources={
            "postgres": PostgresResource(
            host=os.getenv("HOST_DB"),
            port=os.getenv("PORT_DB"),
            user=os.getenv("USER_DB"),
            password=os.getenv("PASSWORD_DB"),
            database=os.getenv("NAME_DB"),
            )
        },
        partition_key="GE"
    )

    assert result.success