from dagster import Definitions, load_assets_from_modules, in_process_executor
from .defs.assets import code_naf, dbt, companies, region_deps, table_scd2
import os 
from .defs.resources import PostgresResource, dbt_resource
from .defs.jobs import api_company_job, code_naf_job, region_deps_job, clean_company_job, scd2_company_job, mart_company_job
from .defs.schedules import api_companies_dayli, clean_companies_dayli, scd2_companies_dayli, mart_companies_dayli, naf_codes_annual, region_deps_annual

# Chargement des assets
code_naf_assets = load_assets_from_modules([code_naf])
dbt_assets = load_assets_from_modules([dbt])
companies_assets = load_assets_from_modules([companies])
region_deps_assets = load_assets_from_modules([region_deps])
scd2_assets = load_assets_from_modules([table_scd2])

# Définition des jobs
all_jobs = [api_company_job, clean_company_job, scd2_company_job, mart_company_job, code_naf_job, region_deps_job]

# Définition des planifications
all_schedules = [api_companies_dayli, clean_companies_dayli, scd2_companies_dayli, mart_companies_dayli, naf_codes_annual, region_deps_annual]

# Définition des sensors
# 

# Définitions de nos assets, schedules, etc
defs = Definitions(
    assets=[*code_naf_assets, *dbt_assets, *companies_assets, *region_deps_assets, *scd2_assets],
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
    jobs=all_jobs,
    schedules=all_schedules
)
