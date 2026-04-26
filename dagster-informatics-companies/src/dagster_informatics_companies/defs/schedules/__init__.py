from dagster import ScheduleDefinition
from ..jobs import company_job, region_deps_job, code_naf_job

# Exécution annuelle pour les codes (début d'année)
naf_codes_annual = ScheduleDefinition(
    job=code_naf_job,
    cron_schedule="0 0 5 1 *", # Tous les cinq janvier de l'année
    execution_timezone="Europe/Paris",
)

# Exécution annuelle pour les régions départements (début d'année)
region_deps_annual = ScheduleDefinition(
    job=region_deps_job,
    cron_schedule="0 0 5 1 *", # Tous les cinq janvier de l'année
    execution_timezone="Europe/Paris",
)

# Exécution journalière
companies_dayli = ScheduleDefinition(
    job=company_job,
    cron_schedule="0 0 * * *", # Tous les jours à minuit
    execution_timezone="Europe/Paris",
)
