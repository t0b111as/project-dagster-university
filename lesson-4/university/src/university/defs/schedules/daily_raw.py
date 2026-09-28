import dagster as dg


@dg.schedule(
    cron_schedule="0 8 * * *",
    target=dg.AssetSelection.groups("raw"),
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def daily_raw(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    return dg.RunRequest()
