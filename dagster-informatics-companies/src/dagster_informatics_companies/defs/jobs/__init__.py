from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition, weekly_partition

deps_region_fr = AssetSelection.assets("deps_regions_fr")
extract_load_companies = AssetSelection.assets("extract_load_companies")

# Exécution qui n'ont aucune partitions temporelles (collecte API)
api_collecte_entreprise = define_asset_job(
name="collecte_chargement_entreprise",
selection=extract_load_companies,
)

# Exécution par partition de chaque catégorie entreprise (PME, GE, ETI)