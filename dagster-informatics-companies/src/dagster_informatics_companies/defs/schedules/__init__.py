from dagster import ScheduleDefinition
from ..jobs import company_job, region_deps_job, code_naf_job

# Exécution annuelle pour les codes (début d'année)
naf_codes_annual = ScheduleDefinition(
    job=code_naf_job,
    cron_schedule="0 0 5 * *", # Tous les ans
)

# # Exécution annuelle pour les régions départements (début d'année)
region_deps_annual = ScheduleDefinition(
    job=region_deps_job,
    cron_schedule="0 0 5 * *", # Tous les ans
)

# Exécution journalière
companies_dayli = ScheduleDefinition(
    job=company_job,
    cron_schedule="0 0 5 * *", # Tous les jours
)
