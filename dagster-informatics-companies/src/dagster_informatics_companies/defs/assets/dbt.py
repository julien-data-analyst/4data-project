# Load librairies
from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

from ..projects import dbt_project

CODE_NAF_SELECTOR = "code_naf"

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]

        if resource_type == 'source' and name=="code_naf":
            return AssetKey("load_naf_codes")
        elif resource_type == 'source' and name=="companies":
            return AssetKey("extract_load_companies")
        else:
            return super().get_asset_key(dbt_resource_props)
    
    def get_group_name(self, dbt_resource_props):
        return dbt_resource_props["fqn"][1]
    
# To materialize in the dagster webserver
@dbt_assets(
    manifest=dbt_project.manifest_path, # Indicates where is placed the manifest
    dagster_dbt_translator=CustomizedDagsterDbtTranslator()
)
def dbt_code_naf(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
