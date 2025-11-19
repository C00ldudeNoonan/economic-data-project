from datetime import datetime
import dagster as dg
from pydantic import Field

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.agents.economy_state_analyzer import (
    EconomicAnalysisResource,
    extract_economy_state_summary,
)


class BacktestConfig(dg.Config):
    """Configuration for backtesting - can be specified at runtime."""

    backtest_date: str = Field(
        description="Backtest date (YYYY-MM-DD), first day of month"
    )
    model_provider: str = Field(
        default="openai",
        description="LLM provider: 'openai', 'gemini', or 'anthropic'",
    )
    model_name: str = Field(
        default="gpt-4-turbo-preview",
        description="LLM model to use for analysis (e.g., 'gpt-4-turbo-preview', 'gpt-4o', 'gpt-3.5-turbo', 'gemini-2.0-flash-exp', 'claude-3-opus-20240229')",
    )
    personality: str = Field(
        default="skeptical",
        description="Analytical personality: 'skeptical' (default, bearish), 'neutral' (balanced), or 'bullish' (optimistic)",
    )


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="backtesting",
    description="Backtest economy state analysis with historical data cutoff",
    deps=[
        dg.AssetKey(["fred_series_latest_aggregates_snapshot"]),
        dg.AssetKey(["leading_econ_return_indicator_snapshot"]),
        dg.AssetKey(["us_sector_summary_snapshot"]),
        dg.AssetKey(["energy_commodities_summary_snapshot"]),
        dg.AssetKey(["input_commodities_summary_snapshot"]),
        dg.AssetKey(["agriculture_commodities_summary_snapshot"]),
    ],
)
def backtest_analyze_economy_state(
    context: dg.AssetExecutionContext,
    config: BacktestConfig,
    md: MotherDuckResource,
    economic_analysis: EconomicAnalysisResource,
) -> dg.MaterializeResult:
    """
    Asset that analyzes economy state for backtesting with historical data cutoff.

    This is the backtest version of analyze_economy_state. It uses snapshot tables
    filtered by backtest_date to simulate what the analysis would have been at that point.

    Configuration (specify at runtime):
        - backtest_date: Date string (YYYY-MM-DD), first day of month
        - model_name: LLM model to use (e.g., 'gpt-4-turbo-preview', 'gpt-4o')

    Returns:
        Dictionary with analysis metadata and results
    """
    context.log.info(
        f"Starting backtest economy state analysis for {config.backtest_date} "
        f"with provider {config.model_provider} and model {config.model_name}..."
    )

    # Update provider and model_name if they differ from current settings
    current_provider = economic_analysis._get_provider()
    if (
        current_provider != config.model_provider
        or economic_analysis.model_name != config.model_name
    ):
        # Access the Field directly to set it
        object.__setattr__(economic_analysis, "provider", config.model_provider)
        economic_analysis.model_name = config.model_name
        economic_analysis.setup_for_execution(context)

    context.log.info("Gathering economic data with cutoff date...")
    economic_data = economic_analysis.get_economic_data(
        md, cutoff_date=config.backtest_date
    )

    context.log.info("Gathering commodity data with cutoff date...")
    commodity_data = economic_analysis.get_commodity_data(
        md, cutoff_date=config.backtest_date
    )

    # Track token usage
    from macro_agents.defs.agents.economy_state_analyzer import _get_token_usage

    initial_history_length = (
        len(economic_analysis._lm.history) if hasattr(economic_analysis, "_lm") else 0
    )

    context.log.info(
        f"Running economy state analysis with historical data (personality: {config.personality})..."
    )
    analysis_result = economic_analysis.economy_state_analyzer(
        economic_data=economic_data,
        commodity_data=commodity_data,
        personality=config.personality,
    )

    # Calculate token usage
    token_usage = _get_token_usage(economic_analysis, initial_history_length, context)

    analysis_timestamp = datetime.now()
    result = {
        "analysis_timestamp": analysis_timestamp.isoformat(),
        "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
        "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
        "backtest_date": config.backtest_date,
        "model_provider": config.model_provider,
        "model_name": config.model_name,
        "personality": config.personality,
        "analysis_content": analysis_result.analysis,
        "data_sources": {
            "economic_data_table": "fred_series_latest_aggregates_snapshot",
            "commodity_data_tables": [
                "energy_commodities_summary_snapshot",
                "input_commodities_summary_snapshot",
                "agriculture_commodities_summary_snapshot",
            ],
        },
    }

    json_result = {
        "analysis_type": "economy_state",
        "analysis_content": result["analysis_content"],
        "analysis_timestamp": result["analysis_timestamp"],
        "analysis_date": result["analysis_date"],
        "analysis_time": result["analysis_time"],
        "backtest_date": result["backtest_date"],
        "model_provider": result["model_provider"],
        "model_name": result["model_name"],
        "personality": result["personality"],
        "data_sources": result["data_sources"],
        "dagster_run_id": context.run_id,
        "dagster_asset_key": str(context.asset_key),
    }

    context.log.info("Writing backtest economy state analysis to database...")
    md.write_results_to_table(
        [json_result],
        output_table="backtest_economy_state_analysis",
        if_exists="append",
        context=context,
    )

    analysis_summary = extract_economy_state_summary(result["analysis_content"])

    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": result["analysis_timestamp"],
        "backtest_date": result["backtest_date"],
        "model_provider": result["model_provider"],
        "model_name": result["model_name"],
        "personality": result["personality"],
        "output_table": "backtest_economy_state_analysis",
        "records_written": 1,
        "data_sources": result["data_sources"],
        "analysis_summary": analysis_summary,
        "analysis_preview": result["analysis_content"][:500]
        if result["analysis_content"]
        else "",
        "token_usage": token_usage,
        "provider": economic_analysis._get_provider(),
    }

    context.log.info(f"Backtest economy state analysis complete: {result_metadata}")
    return dg.MaterializeResult(metadata=result_metadata)
