import dagster as dg

from macro_agents.defs.backtesting.assets import backtesting_assets
from macro_agents.defs.backtesting.jobs import backtesting_jobs


defs = dg.Definitions(
    assets=backtesting_assets,
    jobs=backtesting_jobs,
)
