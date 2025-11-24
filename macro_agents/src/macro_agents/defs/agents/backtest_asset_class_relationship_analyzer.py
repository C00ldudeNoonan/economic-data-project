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

    from macro_agents.defs.agents.economy_state_analyzer import _get_token_usage

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

        context.log.info("Retrieving backtest economy state analysis...")
        economy_state_analysis = get_latest_backtest_economy_state_analysis(
            md, backtest_date, config.model_provider, config.model_name
        )

        if not economy_state_analysis:
            raise ValueError(
                f"No backtest economy state analysis found for {backtest_date} "
                f"with provider {config.model_provider} and model {config.model_name}. Please run backtest_analyze_economy_state first."
            )

        context.log.info("Gathering market performance data with cutoff date...")
        market_data = economic_analysis.get_market_data(md, cutoff_date=backtest_date)

        context.log.info("Gathering correlation data with cutoff date...")
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

        context.log.info("Gathering commodity data with cutoff date...")
        commodity_data = economic_analysis.get_commodity_data(
            md, cutoff_date=backtest_date
        )

        context.log.info(
            "Running asset class relationship analysis with historical data..."
        )
        analysis_result = relationship_analyzer(
            economy_state_analysis=economy_state_analysis,
            market_data=market_data,
            correlation_data=correlation_data,
            commodity_data=commodity_data,
        )

        token_usage = _get_token_usage(
            economic_analysis, initial_history_length, context
        )
        total_token_usage["prompt_tokens"] += token_usage.get("prompt_tokens", 0)
        total_token_usage["completion_tokens"] += token_usage.get(
            "completion_tokens", 0
        )
        total_token_usage["total_tokens"] += token_usage.get("total_tokens", 0)

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

    context.log.info(
        f"Writing {len(all_results)} backtest asset class relationship analysis records to database..."
    )
    md.write_results_to_table(
        all_results,
        output_table="backtest_asset_class_relationship_analysis",
        if_exists="append",
        context=context,
    )

    # Create summary metadata from first result
    first_result = all_results[0]
    analysis_summary = extract_relationship_summary(first_result["analysis_content"])

    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": first_result["analysis_timestamp"],
        "backtest_dates_processed": backtest_dates,
        "num_dates_processed": len(backtest_dates),
        "model_provider": config.model_provider,
        "model_name": config.model_name,
        "output_table": "backtest_asset_class_relationship_analysis",
        "records_written": len(all_results),
        "data_sources": first_result["data_sources"],
        "analysis_summary": analysis_summary,
        "analysis_preview": first_result["analysis_content"][:500]
        if first_result["analysis_content"]
        else "",
        "token_usage": total_token_usage,
        "provider": economic_analysis._get_provider(),
    }

    context.log.info(
        f"Backtest asset class relationship analysis complete: {result_metadata}"
    )
    return dg.MaterializeResult(metadata=result_metadata)
