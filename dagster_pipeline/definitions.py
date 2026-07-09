# Author: Janvi Chitroda | github.com/JanviChitroda24
"""
Dagster Definitions — jobs, schedules, and asset registry.

Expose the streaming pipeline to `dagster dev` and (later) dagster-webserver in production.

Run from repo root (required — see -d . below):
  dagster dev -f dagster_pipeline/definitions.py -d .
"""

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from dagster_pipeline.assets import (
    check_infrastructure,
    load_to_snowflake,
    process_stream,
    produce_trades,
    quality_checks,
)

# Single job materializing all 5 assets in dependency order
streaming_pipeline_job = define_asset_job(
    name="streaming_pipeline",
    selection=[
        check_infrastructure,
        produce_trades,
        process_stream,
        quality_checks,
        load_to_snowflake,
    ],
)

# STOPPED by default — enable manually in Dagster UI when ready
daily_load_schedule = ScheduleDefinition(
    job=streaming_pipeline_job,
    cron_schedule="0 17 * * 1-5",
    default_status=DefaultScheduleStatus.STOPPED,
)

defs = Definitions(
    assets=[
        check_infrastructure,
        produce_trades,
        process_stream,
        quality_checks,
        load_to_snowflake,
    ],
    jobs=[streaming_pipeline_job],
    schedules=[daily_load_schedule],
)
