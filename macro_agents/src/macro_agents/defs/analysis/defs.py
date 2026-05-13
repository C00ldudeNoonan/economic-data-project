import dagster as dg

from macro_agents.defs.analysis.assets import analysis_assets
from macro_agents.defs.analysis.checks import analysis_checks
from macro_agents.defs.analysis.jobs import analysis_jobs
from macro_agents.defs.analysis.resources import analysis_resources
from macro_agents.defs.analysis.schedules import analysis_schedules


defs = dg.Definitions(
    assets=analysis_assets,
    asset_checks=analysis_checks,
    jobs=analysis_jobs,
    schedules=analysis_schedules,
    resources=analysis_resources,
)
