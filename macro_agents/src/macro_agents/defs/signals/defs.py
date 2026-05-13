import dagster as dg

from macro_agents.defs.signals.absorption_ratio import absorption_ratio_signals
from macro_agents.defs.signals.entropy_complexity import entropy_complexity_signals
from macro_agents.defs.signals.fear_greed_composite import fear_greed_signals
from macro_agents.defs.signals.network_correlation import network_correlation_signals
from macro_agents.defs.signals.turbulence_index import turbulence_index_signals
from macro_agents.defs.signals.checks import signal_checks

computed_signal_assets = [
    absorption_ratio_signals,
    turbulence_index_signals,
    fear_greed_signals,
    entropy_complexity_signals,
    network_correlation_signals,
]

absorption_ratio_job = dg.define_asset_job(
    name="absorption_ratio_job",
    tags={"dagster/priority": "5", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("absorption_ratio_signals"),
    description="Absorption ratio signal computation - runs weekly on Mondays at 5 AM EST",
)

turbulence_index_job = dg.define_asset_job(
    name="turbulence_index_job",
    tags={"dagster/priority": "5", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("turbulence_index_signals"),
    description="Turbulence index signal computation - runs weekdays at 5 AM EST",
)

fear_greed_job = dg.define_asset_job(
    name="fear_greed_job",
    tags={"dagster/priority": "5", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("fear_greed_signals"),
    description="Fear & Greed signal computation - runs weekdays at 6 AM EST",
)

entropy_complexity_job = dg.define_asset_job(
    name="entropy_complexity_job",
    tags={"dagster/priority": "5", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("entropy_complexity_signals"),
    description="Entropy complexity signal computation - runs weekdays at 7 AM EST",
)

network_correlation_job = dg.define_asset_job(
    name="network_correlation_job",
    tags={"dagster/priority": "5", "dagster/max_runtime": 2700},
    selection=dg.AssetSelection.assets("network_correlation_signals"),
    description="Network correlation signal computation - runs weekly on Sundays at 4 AM EST",
)

weekly_absorption_ratio_schedule = dg.ScheduleDefinition(
    name="weekly_absorption_ratio_schedule",
    cron_schedule="30 4 * * 1",
    execution_timezone="America/New_York",
    job=absorption_ratio_job,
    description="Weekly absorption ratio computation on Mondays at 4:30 AM EST",
)

weekday_turbulence_index_schedule = dg.ScheduleDefinition(
    name="weekday_turbulence_index_schedule",
    cron_schedule="0 5 * * 1-5",
    execution_timezone="America/New_York",
    job=turbulence_index_job,
    description="Weekday turbulence index computation at 5 AM EST",
)

weekday_fear_greed_schedule = dg.ScheduleDefinition(
    name="weekday_fear_greed_schedule",
    cron_schedule="0 6 * * 1-5",
    execution_timezone="America/New_York",
    job=fear_greed_job,
    description="Weekday fear & greed signal computation at 6 AM EST",
)

weekday_entropy_complexity_schedule = dg.ScheduleDefinition(
    name="weekday_entropy_complexity_schedule",
    cron_schedule="0 7 * * 1-5",
    execution_timezone="America/New_York",
    job=entropy_complexity_job,
    description="Weekday entropy complexity signal computation at 7 AM EST",
)

weekly_network_correlation_schedule = dg.ScheduleDefinition(
    name="weekly_network_correlation_schedule",
    cron_schedule="0 4 * * 0",
    execution_timezone="America/New_York",
    job=network_correlation_job,
    description="Weekly network correlation computation on Sundays at 4 AM EST",
)

signal_jobs = [
    absorption_ratio_job,
    turbulence_index_job,
    fear_greed_job,
    entropy_complexity_job,
    network_correlation_job,
]

signal_schedules = [
    weekly_absorption_ratio_schedule,
    weekday_turbulence_index_schedule,
    weekday_fear_greed_schedule,
    weekday_entropy_complexity_schedule,
    weekly_network_correlation_schedule,
]

defs = dg.Definitions(
    assets=computed_signal_assets,
    asset_checks=signal_checks,
    jobs=signal_jobs,
    schedules=signal_schedules,
)
