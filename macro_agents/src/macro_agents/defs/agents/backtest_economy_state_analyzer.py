from datetime import datetime
from typing import List, Optional
import dagster as dg
from pydantic import Field

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.agents.economy_state_analyzer import (
    EconomicAnalysisResource,
    extract_economy_state_summary,
    _estimate_tokens,
    _check_rate_limit,
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
        description="LLM model to use for analysis (e.g., 'gpt-4-turbo-preview', 'gpt-4o', 'gpt-3.5-turbo', 'gemini-2.0-flash-exp', 'gemini-3-pro-preview', 'claude-3-opus-20240229')",
    )
    personality: str = Field(
        default="skeptical",
        description="Analytical personality: 'skeptical' (default, bearish), 'neutral' (balanced), or 'bullish' (optimistic)",
    )
    use_optimized_models: bool = Field(
        default=False,
        description="If True, use optimized models for backtesting. If False (default), use baseline models to avoid circular dependencies in optimization.",
    )
    token_optimization: bool = Field(
        default=True,
        description="If True (default), use token-saving optimizations (limited series/assets, shorter time periods). If False, use full data for maximum accuracy.",
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
    failed_dates = []
    successful_dates = []

    for backtest_date in backtest_dates:
        context.log.info(
            f"Processing backtest date: {backtest_date} ({backtest_dates.index(backtest_date) + 1}/{len(backtest_dates)})"
        )

        try:
            initial_history_length = (
                len(economic_analysis._lm.history)
                if hasattr(economic_analysis, "_lm")
                else 0
            )

            if config.token_optimization:
                context.log.info(
                    "Gathering economic data with cutoff date (with token optimization, preserving trends)..."
                )
                economic_data = economic_analysis.get_economic_data(
                    md,
                    cutoff_date=backtest_date,
                    max_series=50,
                    latest_month_only=False,
                    max_months_per_series=3,
                )

                context.log.info(
                    "Gathering commodity data with cutoff date (with token optimization, preserving trends)..."
                )
                commodity_data = economic_analysis.get_commodity_data(
                    md,
                    cutoff_date=backtest_date,
                    max_commodities=15,
                    time_periods=["6_months"],
                )

                context.log.info(
                    "Gathering Financial Conditions Index data with cutoff date (with token optimization, preserving trends)..."
                )
                fci_data = economic_analysis.get_financial_conditions_index(
                    md, cutoff_date=backtest_date, max_months=12
                )

                context.log.info(
                    "Gathering housing market data with cutoff date (with token optimization, preserving trends)..."
                )
                housing_data = economic_analysis.get_housing_data(
                    md, cutoff_date=backtest_date, latest_month_only=False, max_months=6
                )

                context.log.info(
                    "Gathering yield curve data with cutoff date (with token optimization, preserving trends)..."
                )
                yield_curve_data = economic_analysis.get_yield_curve_data(
                    md, cutoff_date=backtest_date, max_months=12
                )

                context.log.info(
                    "Gathering economic trends data with cutoff date (with token optimization, preserving trends)..."
                )
                economic_trends = economic_analysis.get_economic_trends(
                    md, cutoff_date=backtest_date, max_months=12
                )
            else:
                context.log.info(
                    "Gathering economic data with cutoff date (full data, no token optimization)..."
                )
                economic_data = economic_analysis.get_economic_data(
                    md, cutoff_date=backtest_date
                )

                context.log.info(
                    "Gathering commodity data with cutoff date (full data, no token optimization)..."
                )
                commodity_data = economic_analysis.get_commodity_data(
                    md, cutoff_date=backtest_date
                )

                context.log.info(
                    "Gathering Financial Conditions Index data with cutoff date (full data, no token optimization)..."
                )
                fci_data = economic_analysis.get_financial_conditions_index(
                    md, cutoff_date=backtest_date
                )

                context.log.info(
                    "Gathering housing market data with cutoff date (full data, no token optimization)..."
                )
                housing_data = economic_analysis.get_housing_data(
                    md, cutoff_date=backtest_date
                )

                context.log.info(
                    "Gathering yield curve data with cutoff date (full data, no token optimization)..."
                )
                yield_curve_data = economic_analysis.get_yield_curve_data(
                    md, cutoff_date=backtest_date
                )

                context.log.info(
                    "Gathering economic trends data with cutoff date (full data, no token optimization)..."
                )
                economic_trends = economic_analysis.get_economic_trends(
                    md, cutoff_date=backtest_date
                )

            provider = economic_analysis._get_provider()
            model_name = economic_analysis._get_model_name()

            combined_input = (
                economic_data
                + "\n"
                + commodity_data
                + "\n"
                + fci_data
                + "\n"
                + housing_data
                + "\n"
                + yield_curve_data
                + "\n"
                + economic_trends
            )
            estimated_tokens = _estimate_tokens(combined_input)
            context.log.info(
                f"Estimated input tokens for {backtest_date}: {estimated_tokens}. "
                f"Checking rate limits for {provider}/{model_name}..."
            )
            _check_rate_limit(provider, model_name, estimated_tokens, context)

            context.log.info(
                f"Running economy state analysis with historical data including FCI, housing, yield curve, and trends (personality: {config.personality})..."
            )
            try:
                analysis_result = analyzer_to_use(
                    economic_data=economic_data,
                    commodity_data=commodity_data,
                    financial_conditions_index=fci_data,
                    housing_data=housing_data,
                    yield_curve_data=yield_curve_data,
                    economic_trends=economic_trends,
                    personality=config.personality,
                )

                context.log.debug(
                    f"Analysis result type: {type(analysis_result)}, "
                    f"has 'analysis' attr: {hasattr(analysis_result, 'analysis')}, "
                    f"dir: {[attr for attr in dir(analysis_result) if not attr.startswith('_')]}"
                )

                if hasattr(analysis_result, "analysis"):
                    analysis_content_value = analysis_result.analysis
                    context.log.debug(
                        f"analysis_result.analysis type: {type(analysis_content_value)}, "
                        f"is None: {analysis_content_value is None}, "
                        f"is empty string: {analysis_content_value == ''}, "
                        f"length: {len(analysis_content_value) if analysis_content_value else 0}"
                    )
                    if (
                        not analysis_content_value
                        or analysis_content_value.strip() == ""
                    ):
                        context.log.warning(
                            f"Empty analysis content detected for {backtest_date}. "
                            f"Full analysis_result object: {analysis_result}"
                        )
                        if hasattr(economic_analysis, "_lm") and hasattr(
                            economic_analysis._lm, "history"
                        ):
                            recent_history = economic_analysis._lm.history[-3:]
                            context.log.warning(
                                f"Recent LLM history (last 3 entries): {recent_history}"
                            )
                            if recent_history:
                                last_entry = recent_history[-1]
                                context.log.warning(
                                    f"Last LLM history entry type: {type(last_entry)}, "
                                    f"keys/attrs: {last_entry.keys() if isinstance(last_entry, dict) else dir(last_entry)[:10]}"
                                )
                                if isinstance(last_entry, dict):
                                    if "messages" in last_entry:
                                        context.log.warning(
                                            f"Last entry messages: {last_entry['messages']}"
                                        )
                                    if "response" in last_entry:
                                        context.log.warning(
                                            f"Last entry response: {last_entry['response']}"
                                        )
                                    if "output" in last_entry:
                                        context.log.warning(
                                            f"Last entry output: {last_entry['output']}"
                                        )
                else:
                    context.log.error(
                        f"analysis_result does not have 'analysis' attribute. "
                        f"Available attributes: {[attr for attr in dir(analysis_result) if not attr.startswith('_')]}"
                    )

            except Exception as e:
                error_msg = str(e)

                if "temperature=0.0" in error_msg and "gpt-5" in error_msg.lower():
                    context.log.error(
                        f"gpt-5 model compatibility error for {backtest_date}: {error_msg}. "
                        f"gpt-5 models only support temperature=1.0. "
                        f"Consider using a different model or setting litellm.drop_params = True."
                    )
                    raise ValueError(
                        f"gpt-5 model compatibility error: {error_msg}. "
                        f"Please use a model that supports temperature=0.0 or configure litellm.drop_params = True."
                    ) from e
                elif (
                    "response_format" in error_msg.lower()
                    or "structured output" in error_msg.lower()
                    or "JSON mode" in error_msg.lower()
                ):
                    context.log.error(
                        f"Structured output format error for {backtest_date}: {error_msg}. "
                        f"Model may not support required response format features."
                    )
                    raise ValueError(
                        f"Model compatibility error: {error_msg}. "
                        f"Please use a model that supports structured output format."
                    ) from e
                else:
                    context.log.error(
                        f"Error during LLM analysis call for {backtest_date}: {error_msg}",
                        exc_info=True,
                    )
                    if hasattr(economic_analysis, "_lm") and hasattr(
                        economic_analysis._lm, "history"
                    ):
                        recent_history = economic_analysis._lm.history[-3:]
                        context.log.debug(
                            f"LLM history at error (last 3 entries): {recent_history}"
                        )
                    raise

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
            successful_dates.append(backtest_date)

            context.log.info(
                f"Successfully processed {backtest_date}, writing result to database..."
            )
            md.write_results_to_table(
                [json_result],
                output_table="backtest_economy_state_analysis",
                if_exists="append",
                context=context,
            )
            context.log.info(f"Result for {backtest_date} written to database")

        except Exception as e:
            error_msg = f"Error processing backtest date {backtest_date}: {str(e)}"
            context.log.error(error_msg)
            failed_dates.append({"date": backtest_date, "error": str(e)})
            context.log.warning(
                f"Continuing with remaining dates. {len(successful_dates)} successful, {len(failed_dates)} failed so far."
            )

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

    if len(all_results) == 0:
        error_msg = f"No backtest dates were successfully processed. All {len(backtest_dates)} date(s) failed."
        context.log.error(error_msg)
        raise ValueError(error_msg)

    context.log.info(
        f"Processed {len(successful_dates)} successful date(s), {len(failed_dates)} failed date(s). "
        f"All successful results have been written to database."
    )

    first_result = all_results[0]
    analysis_content = first_result.get("analysis_content") or ""
    analysis_summary = (
        extract_economy_state_summary(analysis_content) if analysis_content else {}
    )

    result_metadata = {
        "analysis_completed": len(failed_dates) == 0,
        "analysis_timestamp": first_result["analysis_timestamp"],
        "backtest_dates_processed": backtest_dates,
        "successful_dates": successful_dates,
        "failed_dates": failed_dates,
        "num_dates_processed": len(backtest_dates),
        "num_successful": len(successful_dates),
        "num_failed": len(failed_dates),
        "model_provider": config.model_provider,
        "model_name": config.model_name,
        "personality": config.personality,
        "output_table": "backtest_economy_state_analysis",
        "records_written": len(all_results),
        "data_sources": first_result["data_sources"],
        "analysis_summary": analysis_summary,
        "analysis_preview": analysis_content[:500] if analysis_content else "",
        "token_usage": total_token_usage,
        "provider": economic_analysis._get_provider(),
    }

    context.log.info(f"Backtest economy state analysis complete: {result_metadata}")

    if len(failed_dates) > 0:
        degraded_summary = (
            f"Backtest completed with {len(failed_dates)} failure(s) out of {len(backtest_dates)} total date(s). "
            f"Successful results have been saved. Asset status: DEGRADED (partial success)."
        )
        context.log.warning(degraded_summary)
        result_metadata["status"] = "degraded"
        result_metadata["degraded_reason"] = (
            f"{len(failed_dates)} of {len(backtest_dates)} dates failed"
        )
        result_metadata["degraded_failed_dates"] = [f["date"] for f in failed_dates]
        result_metadata["partial_success"] = True
        return dg.MaterializeResult(metadata=result_metadata)

    result_metadata["status"] = "success"
    return dg.MaterializeResult(metadata=result_metadata)
