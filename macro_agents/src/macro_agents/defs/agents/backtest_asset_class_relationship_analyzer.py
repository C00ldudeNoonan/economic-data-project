from typing import Optional
from datetime import datetime
import dagster as dg

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.agents.economy_state_analyzer import EconomicAnalysisResource
from macro_agents.defs.agents.asset_class_relationship_analyzer import (
    AssetClassRelationshipModule,
    extract_relationship_summary,
)
from macro_agents.defs.agents.backtest_economy_state_analyzer import (
    BacktestConfig,
    backtest_analyze_economy_state,
    get_backtest_dates,
)


def get_latest_backtest_economy_state_analysis(
    md_resource: MotherDuckResource,
    backtest_date: str,
    model_provider: str,
    model_name: str,
) -> Optional[str]:
    """Get the latest backtest economy state analysis from the database."""
    query = f"""
    SELECT analysis_content
    FROM backtest_economy_state_analysis
    WHERE analysis_type = 'economy_state'
        AND backtest_date = '{backtest_date}'
        AND model_provider = '{model_provider}'
        AND model_name = '{model_name}'
    ORDER BY analysis_timestamp DESC
    LIMIT 1
    """

    df = md_resource.execute_query(query, read_only=True)
    if df.is_empty():
        return None

    return df[0, "analysis_content"]


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="backtesting",
    description="Backtest asset class relationship analysis with historical data cutoff",
    deps=[
        backtest_analyze_economy_state,
        dg.AssetKey(["us_sector_summary_snapshot"]),
        dg.AssetKey(["leading_econ_return_indicator_snapshot"]),
        dg.AssetKey(["energy_commodities_summary_snapshot"]),
        dg.AssetKey(["input_commodities_summary_snapshot"]),
        dg.AssetKey(["agriculture_commodities_summary_snapshot"]),
    ],
)
def backtest_analyze_asset_class_relationships(
    context: dg.AssetExecutionContext,
    config: BacktestConfig,
    md: MotherDuckResource,
    economic_analysis: EconomicAnalysisResource,
    gcs: GCSResource,
) -> dg.MaterializeResult:
    """
    Asset that analyzes relationships between asset classes for backtesting.

    This is the backtest version of analyze_asset_class_relationships.

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
        f"Starting backtest asset class relationship analysis for {len(backtest_dates)} date(s) "
        f"with provider {config.model_provider} and model {config.model_name}..."
    )

    # Setup resource with overrides (respects frozen nature of resource)
    economic_analysis.setup_for_execution(
        context,
        provider_override=config.model_provider,
        model_name_override=config.model_name,
    )

    # Prepare analyzer (only need to do this once for all dates)
    relationship_analyzer = None
    if config.use_optimized_models and economic_analysis.use_optimized_models:
        relationship_analyzer = economic_analysis.load_optimized_module(
            module_name="asset_class_relationship",
            md_resource=md,
            gcs_resource=gcs,
            context=context,
            personality=None,  # Asset class relationship doesn't use personality
        )
        if relationship_analyzer:
            context.log.info("Using optimized model for backtest")
        else:
            context.log.info("No optimized model found, falling back to baseline model")

    if relationship_analyzer is None:
        relationship_analyzer = AssetClassRelationshipModule()

    from macro_agents.defs.agents.economy_state_analyzer import (
        _get_token_usage,
        _calculate_cost,
        _estimate_tokens,
        _check_rate_limit,
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

            context.log.info("Retrieving backtest economy state analysis...")
            economy_state_analysis = get_latest_backtest_economy_state_analysis(
                md, backtest_date, config.model_provider, config.model_name
            )

            if not economy_state_analysis:
                raise ValueError(
                    f"No backtest economy state analysis found for {backtest_date} "
                    f"with provider {config.model_provider} and model {config.model_name}. Please run backtest_analyze_economy_state first."
                )

            if config.token_optimization:
                context.log.info(
                    "Gathering market performance data with cutoff date (with token optimization)..."
                )
                market_data = economic_analysis.get_market_data(
                    md,
                    cutoff_date=backtest_date,
                    max_assets=20,
                    time_periods=["6_months"],
                )

                context.log.info(
                    "Gathering correlation data with cutoff date (with token optimization)..."
                )
                try:
                    correlation_data = economic_analysis.get_correlation_data(
                        md,
                        sample_size=50,
                        sampling_strategy="top_correlations",
                        cutoff_date=backtest_date,
                    )
                except Exception as e:
                    context.log.warning(
                        f"Could not retrieve correlation data: {e}. Continuing without it."
                    )
                    correlation_data = ""

                context.log.info(
                    "Gathering commodity data with cutoff date (with token optimization)..."
                )
                commodity_data = economic_analysis.get_commodity_data(
                    md,
                    cutoff_date=backtest_date,
                    max_commodities=15,
                    time_periods=["6_months"],
                )
            else:
                context.log.info(
                    "Gathering market performance data with cutoff date (full data, no token optimization)..."
                )
                market_data = economic_analysis.get_market_data(
                    md, cutoff_date=backtest_date
                )

                context.log.info(
                    "Gathering correlation data with cutoff date (full data, no token optimization)..."
                )
                try:
                    correlation_data = economic_analysis.get_correlation_data(
                        md,
                        sample_size=100,
                        sampling_strategy="top_correlations",
                        cutoff_date=backtest_date,
                    )
                except Exception as e:
                    context.log.warning(
                        f"Could not retrieve correlation data: {e}. Continuing without it."
                    )
                    correlation_data = ""

                context.log.info(
                    "Gathering commodity data with cutoff date (full data, no token optimization)..."
                )
                commodity_data = economic_analysis.get_commodity_data(
                    md, cutoff_date=backtest_date
                )

            provider = economic_analysis._get_provider()
            model_name = economic_analysis._get_model_name()

            combined_input = (
                economy_state_analysis
                + "\n"
                + market_data
                + "\n"
                + correlation_data
                + "\n"
                + commodity_data
            )
            estimated_tokens = _estimate_tokens(combined_input)
            context.log.info(
                f"Estimated input tokens for {backtest_date}: {estimated_tokens}. "
                f"Checking rate limits for {provider}/{model_name}..."
            )
            _check_rate_limit(provider, model_name, estimated_tokens, context)

            context.log.info(
                "Running asset class relationship analysis with historical data..."
            )
            try:
                analysis_result = relationship_analyzer(
                    economy_state_analysis=economy_state_analysis,
                    market_data=market_data,
                    correlation_data=correlation_data,
                    commodity_data=commodity_data,
                )

                context.log.debug(
                    f"Analysis result type: {type(analysis_result)}, "
                    f"has 'relationship_analysis' attr: {hasattr(analysis_result, 'relationship_analysis')}, "
                    f"dir: {[attr for attr in dir(analysis_result) if not attr.startswith('_')]}"
                )

                if hasattr(analysis_result, "relationship_analysis"):
                    analysis_content_value = analysis_result.relationship_analysis
                    context.log.debug(
                        f"analysis_result.relationship_analysis type: {type(analysis_content_value)}, "
                        f"is None: {analysis_content_value is None}, "
                        f"is empty string: {analysis_content_value == ''}, "
                        f"length: {len(analysis_content_value) if analysis_content_value else 0}"
                    )
                    if (
                        not analysis_content_value
                        or analysis_content_value.strip() == ""
                    ):
                        context.log.warning(
                            f"Empty relationship analysis content detected for {backtest_date}. "
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
                        f"analysis_result does not have 'relationship_analysis' attribute. "
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
                "analysis_type": "asset_class_relationships",
                "analysis_content": analysis_result.relationship_analysis,
                "analysis_timestamp": analysis_timestamp.isoformat(),
                "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
                "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
                "backtest_date": backtest_date,
                "model_provider": config.model_provider,
                "model_name": config.model_name,
                "data_sources": {
                    "economy_state_table": "backtest_economy_state_analysis",
                    "market_data_table": "us_sector_summary_snapshot",
                    "correlation_data_table": "leading_econ_return_indicator_snapshot",
                    "commodity_data_tables": [
                        "energy_commodities_summary_snapshot",
                        "input_commodities_summary_snapshot",
                        "agriculture_commodities_summary_snapshot",
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
                output_table="backtest_asset_class_relationship_analysis",
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
        extract_relationship_summary(analysis_content) if analysis_content else {}
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
        "output_table": "backtest_asset_class_relationship_analysis",
        "records_written": len(all_results),
        "data_sources": first_result["data_sources"],
        "analysis_summary": analysis_summary,
        "analysis_preview": analysis_content[:500] if analysis_content else "",
        "token_usage": total_token_usage,
        "provider": economic_analysis._get_provider(),
    }

    context.log.info(
        f"Backtest asset class relationship analysis complete: {result_metadata}"
    )

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
