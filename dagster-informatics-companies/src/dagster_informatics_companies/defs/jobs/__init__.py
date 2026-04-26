from dagster import AssetSelection, define_asset_job
# from ..partitions import monthly_partition, weekly_partition

# Les assets pour les départements et régions françaises
deps_region_fr = AssetSelection.groups("deps_regions_fr")
dim_region_deps = AssetSelection.assets("dim_region_deps")

# Les assets pour les entreprises
companies_assets = AssetSelection.assets("extract_load_companies", "companies_cleaned", "fact_companies")

# Les assets pour les codes naf informatiques
code_naf_informatics = AssetSelection.groups("naf_codes")
dim_code_naf_informatics = AssetSelection.assets("dim_codes_naf_informatics")

# Company job (à ajouter partition par catégorie entreprise)
company_job = define_asset_job(
name="company_job",
selection=companies_assets,
)

# Code naf job
code_naf_job = define_asset_job(
    name='code_naf_job',
    selection=(code_naf_informatics | dim_code_naf_informatics)
)

# Région et département job
region_deps_job = define_asset_job(
    name='region_deps_job',
    selection=(deps_region_fr | dim_region_deps)
)