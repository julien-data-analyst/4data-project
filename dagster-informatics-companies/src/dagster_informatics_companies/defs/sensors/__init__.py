from dagster import sensor, RunRequest, SkipReason, AssetKey, AssetExecutionContext
from dagster import DagsterEventType

@sensor(asset_selection=[
    "companies_time_series"
])
def fact_companies_sensor(context: AssetExecutionContext):
    """
    
    """
    events = context.instance.get_event_records(
        event_type=DagsterEventType.ASSET_MATERIALIZATION,
        asset_key=AssetKey("fact_companies"),
        limit=1
    )

    if not events:
        return SkipReason("fact_companies n'est pas encore matérialisée")

    # Savoir s'il y a eu un nouvel événement
    last_event_id = events[0].storage_id

    if context.cursor == str(last_event_id):
        return SkipReason("Pas de nouvelle matérialisation")

    context.update_cursor(str(last_event_id))

    return RunRequest(
        run_key=str(last_event_id),
        run_config={}
    )