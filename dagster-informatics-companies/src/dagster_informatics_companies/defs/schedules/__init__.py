from dagster import ScheduleDefinition
from ..jobs import trip_update_job, weekly_update_job

trip_update_schedule = ScheduleDefinition(
    job=trip_update_job,
    cron_schedule="0 0 5 * *", # Tous les 5 du mois à minuit
)

# Pour spécifiquement le weekly
weekly_update_schedule = ScheduleDefinition(
    job=weekly_update_job,
    cron_schedule="0 0 * * 1", # Tous les lundi à minuit
)
