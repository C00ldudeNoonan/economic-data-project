from datetime import datetime
from typing import List, Optional
import dagster as dg
from pydantic import Field

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.agents.economy_state_analyzer import (
    EconomicAnalysisResource,
    extract_economy_state_summary,
)


class BacktestConfig(dg.Config):
    """Configuration for backtesting - can be specified at runtime."""

    backtest_date: Optional[str] = Field(
        default=None,
        description="Single backtest date (YYYY-MM-DD), first day of month. If provided, only this date will be processed. Mutually exclusive with backtest_date_start/end.",
    )
    backtest_date_start: Optional[str] = Field(
        default=None,
        description="Start date for date range backtesting (YYYY-MM-DD), first day of month. If provided with backtest_date_end, processes all months in range.",
    )
    backtest_date_end: Optional[str] = Field(
        default=None,
        description="End date for date range backtesting (YYYY-MM-DD), first day of month. If provided with backtest_date_start, processes all months in range.",
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
    use_optimized_models: bool = Field(
        default=False,
        description="If True, use optimized models for backtesting. If False (default), use baseline models to avoid circular dependencies in optimization.",
    )


def get_backtest_dates(config: BacktestConfig) -> List[str]:
    """
    Get list of backtest dates to process based on config.

    Returns:
        List of date strings (YYYY-MM-DD) representing first day of each month to process.
    """
    if config.backtest_date:
        # Single date mode
        return [config.backtest_date]
    elif config.backtest_date_start and config.backtest_date_end:
        # Date range mode - generate all months between start and end
        start = datetime.strptime(config.backtest_date_start, "%Y-%m-%d")
        end = datetime.strptime(config.backtest_date_end, "%Y-%m-%d")

        if start > end:
            raise ValueError(
                f"backtest_date_start ({config.backtest_date_start}) must be <= backtest_date_end ({config.backtest_date_end})"
            )

        dates = []
        current = start.replace(day=1)  # Start from first day of month
        end_first = end.replace(day=1)  # End at first day of month

        while current <= end_first:
            dates.append(current.strftime("%Y-%m-%d"))
            # Move to first day of next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)

        return dates
    else:
        raise ValueError(
            "Must provide either 'backtest_date' or both 'backtest_date_start' and 'backtest_date_end'"
        )


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="backtesting",
    description="Backtest economy state analysis with historical data cutoff",
    deps=[
        dg.AssetKey(["fred_series_latest_aggregates_snapshot"]),
        dg.AssetKey(["fred_monthly_diff"]),
        dg.AssetKey(["leading_econ_return_indicator_snapshot"]),
        dg.AssetKey(["us_sector_summary_snapshot"]),
        dg.AssetKey(["major_indicies_summary"]),
        dg.AssetKey(["energy_commodities_summary_snapshot"]),
        dg.AssetKey(["input_commodities_summary_snapshot"]),
        dg.AssetKey(["agriculture_commodities_summary_snapshot"]),
        dg.AssetKey(["financial_conditions_index"]),
        dg.AssetKey(["housing_inventory_latest_aggregates"]),
        dg.AssetKey(["housing_mortgage_rates"]),
        dg.AssetKey(["stg_treasury_yields"]),
    ],
)
def backtest_analyze_economy_state(
    context: dg.AssetExecutionContext,
    config: BacktestConfig,
    md: MotherDuckResource,
    economic_analysis: EconomicAnalysisResource,
    gcs: GCSResource,
) -> dg.MaterializeResult:
    """
    Asset that analyzes economy state for backtesting with historical data cutoff.

    This is the backtest version of analyze_economy_state. It uses snapshot tables
    filtered by backtest_date to simulate what the analysis would have been at that point.

    Configuration (specify at runtime):
        - backtest_date: Single date string (YYYY-MM-DD), first day of month, OR
        - backtest_date_start and backtest_date_end: Date range (YYYY-MM-DD), first day of month
        - model_name: LLM model to use (e.g., 'gpt-4-turbo-preview', 'gpt-4o', 'claude-3-5-haiku-20241022')

    Returns:
        Dictionary with analysis metadata and results
    """
    # Get list of dates to process
    backtest_dates = get_backtest_dates(config)

    context.log.info(
        f"Starting backtest economy state analysis for {len(backtest_dates)} date(s) "
        f"with provider {config.model_provider} and model {config.model_name}..."
    )

    # Setup resource with overrides (respects frozen nature of resource)
    economic_analysis.setup_for_execution(
        context,
        provider_override=config.model_provider,
        model_name_override=config.model_name,
    )

    # Prepare analyzer (only need to do this once for all dates)
    optimized_analyzer = None
    if config.use_optimized_models and economic_analysis.use_optimized_models:
        optimized_analyzer = economic_analysis.load_optimized_module(
            module_name="economy_state",
            md_resource=md,
            gcs_resource=gcs,
            context=context,
            personality=config.personality,
        )
        if optimized_analyzer:
            context.log.info(
                f"Using optimized model for backtest (personality: {config.personality})"
            )
        else:
            context.log.info("No optimized model found, falling back to baseline model")

    analyzer_to_use = (
        optimized_analyzer
        if optimized_analyzer
        else economic_analysis.economy_state_analyzer
    )

    from macro_agents.defs.agents.economy_state_analyzer import (
        _get_token_usage,
        _calculate_cost,
    )

    provider = economic_analysis._get_provider()
    model_name = economic_analysis._get_model_name()

    # Process each date
    all_results = []
    total_token_usage = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }

    for backtest_date in backtest_dates:
        context.log.info(
            f"Processing backtest date: {backtest_date} ({backtest_dates.index(backtest_date) + 1}/{len(backtest_dates)})"
        )

        initial_history_length = (
            len(economic_analysis._lm.history)
            if hasattr(economic_analysis, "_lm")
            else 0
        )

        context.log.info("Gathering economic data with cutoff date...")
        economic_data = economic_analysis.get_economic_data(
            md, cutoff_date=backtest_date
        )

        context.log.info("Gathering commodity data with cutoff date...")
        commodity_data = economic_analysis.get_commodity_data(
            md, cutoff_date=backtest_date
        )

        context.log.info(
            "Gathering Financial Conditions Index data with cutoff date..."
        )
        fci_data = economic_analysis.get_financial_conditions_index(
            md, cutoff_date=backtest_date
        )

        context.log.info("Gathering housing market data with cutoff date...")
        housing_data = economic_analysis.get_housing_data(md, cutoff_date=backtest_date)

        context.log.info("Gathering yield curve data with cutoff date...")
        yield_curve_data = economic_analysis.get_yield_curve_data(
            md, cutoff_date=backtest_date
        )

        context.log.info("Gathering economic trends data with cutoff date...")
        economic_trends = economic_analysis.get_economic_trends(
            md, cutoff_date=backtest_date
        )

        context.log.info(
            f"Running economy state analysis with historical data including FCI, housing, yield curve, and trends (personality: {config.personality})..."
        )
        analysis_result = analyzer_to_use(
            economic_data=economic_data,
            commodity_data=commodity_data,
            financial_conditions_index=fci_data,
            housing_data=housing_data,
            yield_curve_data=yield_curve_data,
            economic_trends=economic_trends,
            personality=config.personality,
        )

        token_usage = _get_token_usage(
            economic_analysis, initial_history_length, context
        )

        if (
            "total_cost_usd" not in token_usage
            or token_usage.get("total_cost_usd", 0) == 0
        ):
            provider = economic_analysis._get_provider()
            model_name = economic_analysis._get_model_name()
            cost_data = _calculate_cost(
                provider=provider,
                model_name=model_name,
                prompt_tokens=token_usage.get("prompt_tokens", 0),
                completion_tokens=token_usage.get("completion_tokens", 0),
            )
            token_usage.update(cost_data)

        total_token_usage["prompt_tokens"] += token_usage.get("prompt_tokens", 0)
        total_token_usage["completion_tokens"] += token_usage.get(
            "completion_tokens", 0
        )
        total_token_usage["total_tokens"] += token_usage.get("total_tokens", 0)
        if "total_cost_usd" in token_usage:
            total_token_usage["total_cost_usd"] = total_token_usage.get(
                "total_cost_usd", 0
            ) + token_usage.get("total_cost_usd", 0)
        if "prompt_cost_usd" in token_usage:
            total_token_usage["prompt_cost_usd"] = total_token_usage.get(
                "prompt_cost_usd", 0
            ) + token_usage.get("prompt_cost_usd", 0)
        if "completion_cost_usd" in token_usage:
            total_token_usage["completion_cost_usd"] = total_token_usage.get(
                "completion_cost_usd", 0
            ) + token_usage.get("completion_cost_usd", 0)

        analysis_timestamp = datetime.now()
        json_result = {
            "analysis_type": "economy_state",
            "analysis_content": analysis_result.analysis,
            "analysis_timestamp": analysis_timestamp.isoformat(),
            "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
            "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
            "backtest_date": backtest_date,
            "model_provider": config.model_provider,
            "model_name": config.model_name,
            "personality": config.personality,
            "data_sources": {
                "economic_data_table": "fred_series_latest_aggregates_snapshot",
                "commodity_data_tables": [
                    "energy_commodities_summary_snapshot",
                    "input_commodities_summary_snapshot",
                    "agriculture_commodities_summary_snapshot",
                ],
                "financial_conditions_index_table": "financial_conditions_index",
                "housing_data_tables": [
                    "housing_inventory_latest_aggregates",
                    "housing_mortgage_rates",
                ],
                "yield_curve_table": "stg_treasury_yields",
                "economic_trends_table": "fred_monthly_diff",
                "market_data_tables": [
                    "us_sector_summary_snapshot",
                    "major_indicies_summary",
                ],
            },
            "dagster_run_id": context.run_id,
            "dagster_asset_key": str(context.asset_key),
        }

        all_results.append(json_result)

    if (
        "total_cost_usd" not in total_token_usage
        or total_token_usage.get("total_cost_usd", 0) == 0
    ):
        cost_data = _calculate_cost(
            provider=provider,
            model_name=model_name,
            prompt_tokens=total_token_usage.get("prompt_tokens", 0),
            completion_tokens=total_token_usage.get("completion_tokens", 0),
        )
        total_token_usage.update(cost_data)

    context.log.info(
        f"Writing {len(all_results)} backtest economy state analysis records to database..."
    )
    md.write_results_to_table(
        all_results,
        output_table="backtest_economy_state_analysis",
        if_exists="append",
        context=context,
    )

    # Create summary metadata from first result
    first_result = all_results[0]
    analysis_summary = extract_economy_state_summary(first_result["analysis_content"])

    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": first_result["analysis_timestamp"],
        "backtest_dates_processed": backtest_dates,
        "num_dates_processed": len(backtest_dates),
        "model_provider": config.model_provider,
        "model_name": config.model_name,
        "personality": config.personality,
        "output_table": "backtest_economy_state_analysis",
        "records_written": len(all_results),
        "data_sources": first_result["data_sources"],
        "analysis_summary": analysis_summary,
        "analysis_preview": first_result["analysis_content"][:500]
        if first_result["analysis_content"]
        else "",
        "token_usage": total_token_usage,
        "provider": economic_analysis._get_provider(),
    }

    context.log.info(f"Backtest economy state analysis complete: {result_metadata}")
    return dg.MaterializeResult(metadata=result_metadata)
