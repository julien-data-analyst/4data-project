from dagster import Definitions, load_assets_from_modules, in_process_executor
from .defs.assets import code_naf, dbt

from .defs.resources import PostgresResource, dbt_resource
# Chargement des assets
code_naf_assets = load_assets_from_modules([code_naf])
dbt_assets = load_assets_from_modules([dbt])

# Définitions de nos assets, schedules, etc
defs = Definitions(
    assets=[*code_naf_assets, *dbt_assets],
    executor=in_process_executor,
   resources={
        "postgres": PostgresResource(
            host="postgres",
            port=5432,
            user="postgres",
            password="postgres",
            database="annual_french_informatics_companies",
        ),
        "dbt": dbt_resource
    },
)
