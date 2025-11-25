from typing import Optional
from datetime import datetime
import dagster as dg

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.agents.economy_state_analyzer import EconomicAnalysisResource
from macro_agents.defs.agents.investment_recommendations import (
    InvestmentRecommendationsModule,
    extract_recommendations_summary,
)
from macro_agents.defs.agents.backtest_economy_state_analyzer import (
    BacktestConfig,
    backtest_analyze_economy_state,
    get_backtest_dates,
)
from macro_agents.defs.agents.backtest_asset_class_relationship_analyzer import (
    backtest_analyze_asset_class_relationships,
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


def get_latest_backtest_relationship_analysis(
    md_resource: MotherDuckResource,
    backtest_date: str,
    model_provider: str,
    model_name: str,
) -> Optional[str]:
    """Get the latest backtest asset class relationship analysis from the database."""
    query = f"""
    SELECT analysis_content
    FROM backtest_asset_class_relationship_analysis
    WHERE analysis_type = 'asset_class_relationships'
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
    description="Backtest investment recommendations generation with historical data cutoff",
    deps=[
        backtest_analyze_economy_state,
        backtest_analyze_asset_class_relationships,
    ],
)
def backtest_generate_investment_recommendations(
    context: dg.AssetExecutionContext,
    config: BacktestConfig,
    md: MotherDuckResource,
    economic_analysis: EconomicAnalysisResource,
    gcs: GCSResource,
) -> dg.MaterializeResult:
    """
    Asset that generates investment recommendations for backtesting.

    This is the backtest version of generate_investment_recommendations.

    Configuration (specify at runtime):
        - backtest_date: Single date string (YYYY-MM-DD), first day of month, OR
        - backtest_date_start and backtest_date_end: Date range (YYYY-MM-DD), first day of month
        - model_name: LLM model to use (e.g., 'gpt-4-turbo-preview', 'gpt-4o', 'claude-3-5-haiku-20241022')

    Returns:
        Dictionary with recommendations metadata and results
    """
    # Get list of dates to process
    backtest_dates = get_backtest_dates(config)

    context.log.info(
        f"Starting backtest investment recommendations generation for {len(backtest_dates)} date(s) "
        f"with provider {config.model_provider} and model {config.model_name}..."
    )

    # Setup resource with overrides (respects frozen nature of resource)
    economic_analysis.setup_for_execution(
        context,
        provider_override=config.model_provider,
        model_name_override=config.model_name,
    )

    # Prepare recommendations generator (only need to do this once for all dates)
    recommendations_generator = None
    if config.use_optimized_models and economic_analysis.use_optimized_models:
        recommendations_generator = economic_analysis.load_optimized_module(
            module_name="investment_recommendations",
            md_resource=md,
            gcs_resource=gcs,
            context=context,
            personality=config.personality,
        )
        if recommendations_generator and hasattr(
            recommendations_generator, "personality"
        ):
            if recommendations_generator.personality != config.personality:
                context.log.warning(
                    f"Optimized model personality ({recommendations_generator.personality}) "
                    f"doesn't match config ({config.personality}), using baseline"
                )
                recommendations_generator = None

        if recommendations_generator:
            context.log.info(
                f"Using optimized model for backtest (personality: {config.personality})"
            )
        else:
            context.log.info("No optimized model found, falling back to baseline model")

    if recommendations_generator is None:
        recommendations_generator = InvestmentRecommendationsModule(
            personality=config.personality
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

            context.log.info("Retrieving backtest economy state analysis...")
            economy_state_analysis = get_latest_backtest_economy_state_analysis(
                md, backtest_date, config.model_provider, config.model_name
            )

            if not economy_state_analysis:
                raise ValueError(
                    f"No backtest economy state analysis found for {backtest_date} "
                    f"with provider {config.model_provider} and model {config.model_name}. Please run backtest_analyze_economy_state first."
                )

            context.log.info("Retrieving backtest asset class relationship analysis...")
            relationship_analysis = get_latest_backtest_relationship_analysis(
                md, backtest_date, config.model_provider, config.model_name
            )

            if not relationship_analysis:
                raise ValueError(
                    f"No backtest asset class relationship analysis found for {backtest_date} "
                    f"with provider {config.model_provider} and model {config.model_name}. Please run backtest_analyze_asset_class_relationships first."
                )

            context.log.info(
                f"Generating backtest investment recommendations (personality: {config.personality})..."
            )
            try:
                recommendations_result = recommendations_generator(
                    economy_state_analysis=economy_state_analysis,
                    asset_class_relationship_analysis=relationship_analysis,
                    personality=config.personality,
                )

                context.log.debug(
                    f"Recommendations result type: {type(recommendations_result)}, "
                    f"has 'recommendations' attr: {hasattr(recommendations_result, 'recommendations')}, "
                    f"dir: {[attr for attr in dir(recommendations_result) if not attr.startswith('_')]}"
                )

                if hasattr(recommendations_result, "recommendations"):
                    recommendations_content_value = (
                        recommendations_result.recommendations
                    )
                    context.log.debug(
                        f"recommendations_result.recommendations type: {type(recommendations_content_value)}, "
                        f"is None: {recommendations_content_value is None}, "
                        f"is empty string: {recommendations_content_value == ''}, "
                        f"length: {len(recommendations_content_value) if recommendations_content_value else 0}"
                    )
                    if (
                        not recommendations_content_value
                        or recommendations_content_value.strip() == ""
                    ):
                        context.log.warning(
                            f"Empty recommendations content detected for {backtest_date}. "
                            f"Full recommendations_result object: {recommendations_result}"
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
                        f"recommendations_result does not have 'recommendations' attribute. "
                        f"Available attributes: {[attr for attr in dir(recommendations_result) if not attr.startswith('_')]}"
                    )

            except Exception as e:
                context.log.error(
                    f"Error during LLM recommendations call for {backtest_date}: {str(e)}",
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
                "analysis_type": "investment_recommendations",
                "recommendations_content": recommendations_result.recommendations,
                "analysis_timestamp": analysis_timestamp.isoformat(),
                "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
                "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
                "backtest_date": backtest_date,
                "model_provider": config.model_provider,
                "model_name": config.model_name,
                "personality": config.personality,
                "data_sources": {
                    "economy_state_table": "backtest_economy_state_analysis",
                    "relationship_analysis_table": "backtest_asset_class_relationship_analysis",
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
                output_table="backtest_investment_recommendations",
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
    recommendations_content = first_result.get("recommendations_content") or ""
    recommendations_summary = (
        extract_recommendations_summary(recommendations_content)
        if recommendations_content
        else {}
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
        "output_table": "backtest_investment_recommendations",
        "records_written": len(all_results),
        "data_sources": first_result["data_sources"],
        "recommendations_summary": recommendations_summary,
        "recommendations_preview": (
            recommendations_content[:500] if recommendations_content else ""
        ),
        "token_usage": total_token_usage,
        "provider": economic_analysis._get_provider(),
    }

    context.log.info(
        f"Backtest investment recommendations generation complete: {result_metadata}"
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
