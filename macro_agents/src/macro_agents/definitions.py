import dagster as dg

from macro_agents.defs import (
    analysis,
    asset_failure_sensor,
    backtesting,
    shared_resources,
    signals,
)
from macro_agents.defs.domains import (
    calendars,
    data_infra,
    fomc_transcripts,
    housing,
    macro,
    markets,
    sec,
    social,
    social_sentiment,
    social_tickers,
)
from macro_agents.defs.telemetry import telemetry
from macro_agents.defs import transformation


defs = dg.Definitions.merge(
    shared_resources.defs,
    markets.defs,
    calendars.defs,
    macro.defs,
    housing.defs,
    social.defs,
    social_sentiment.defs,
    social_tickers.defs,
    sec.defs,
    fomc_transcripts.defs,
    data_infra.defs,
    telemetry.defs,
    transformation.defs,
    analysis.defs,
    backtesting.defs,
    signals.defs,
    asset_failure_sensor.defs,
)
