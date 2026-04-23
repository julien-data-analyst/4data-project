from dagster import Definitions, load_assets_from_modules, in_process_executor
from .defs.assets import code_naf, dbt, companies
import os 
from .defs.resources import PostgresResource, dbt_resource
# Chargement des assets
code_naf_assets = load_assets_from_modules([code_naf])
dbt_assets = load_assets_from_modules([dbt])
companies_assets = load_assets_from_modules([companies])

# Définitions de nos assets, schedules, etc
defs = Definitions(
    assets=[*code_naf_assets, *dbt_assets, *companies_assets],
    executor=in_process_executor,
    resources={
        "postgres": PostgresResource(
            host=os.getenv("HOST_DB"),
            port=os.getenv("PORT_DB"),
            user=os.getenv("USER_DB"),
            password=os.getenv("PASSWORD_DB"),
            database=os.getenv("NAME_DB"),
        ),
        "dbt": dbt_resource
    },
)
