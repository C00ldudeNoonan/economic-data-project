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

    Returns:
        Dictionary with recommendations metadata and results
    """
    context.log.info(
        f"Starting backtest investment recommendations generation for {config.backtest_date} "
        f"with provider {config.model_provider} and model {config.model_name}..."
    )

    current_provider = economic_analysis._get_provider()
    if (
        current_provider != config.model_provider
        or economic_analysis.model_name != config.model_name
    ):
        object.__setattr__(economic_analysis, "provider", config.model_provider)
        economic_analysis.model_name = config.model_name
        economic_analysis.setup_for_execution(context)

    context.log.info("Retrieving backtest economy state analysis...")
    economy_state_analysis = get_latest_backtest_economy_state_analysis(
        md, config.backtest_date, config.model_provider, config.model_name
    )

    if not economy_state_analysis:
        raise ValueError(
            f"No backtest economy state analysis found for {config.backtest_date} "
            f"with provider {config.model_provider} and model {config.model_name}. Please run backtest_analyze_economy_state first."
        )

    context.log.info("Retrieving backtest asset class relationship analysis...")
    relationship_analysis = get_latest_backtest_relationship_analysis(
        md, config.backtest_date, config.model_provider, config.model_name
    )

    if not relationship_analysis:
        raise ValueError(
            f"No backtest asset class relationship analysis found for {config.backtest_date} "
            f"with provider {config.model_provider} and model {config.model_name}. Please run backtest_analyze_asset_class_relationships first."
        )

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

    from macro_agents.defs.agents.economy_state_analyzer import _get_token_usage

    initial_history_length = (
        len(economic_analysis._lm.history) if hasattr(economic_analysis, "_lm") else 0
    )

    context.log.info(
        f"Generating backtest investment recommendations (personality: {config.personality})..."
    )
    recommendations_result = recommendations_generator(
        economy_state_analysis=economy_state_analysis,
        asset_class_relationship_analysis=relationship_analysis,
        personality=config.personality,
    )

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
        "recommendations_content": recommendations_result.recommendations,
        "data_sources": {
            "economy_state_table": "backtest_economy_state_analysis",
            "relationship_analysis_table": "backtest_asset_class_relationship_analysis",
        },
    }

    json_result = {
        "analysis_type": "investment_recommendations",
        "recommendations_content": result["recommendations_content"],
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

    context.log.info("Writing backtest investment recommendations to database...")
    md.write_results_to_table(
        [json_result],
        output_table="backtest_investment_recommendations",
        if_exists="append",
        context=context,
    )

    recommendations_summary = extract_recommendations_summary(
        result["recommendations_content"]
    )

    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": result["analysis_timestamp"],
        "backtest_date": result["backtest_date"],
        "model_provider": result["model_provider"],
        "model_name": result["model_name"],
        "personality": result["personality"],
        "output_table": "backtest_investment_recommendations",
        "records_written": 1,
        "data_sources": result["data_sources"],
        "recommendations_summary": recommendations_summary,
        "recommendations_preview": result["recommendations_content"][:500]
        if result["recommendations_content"]
        else "",
        "token_usage": token_usage,
        "provider": economic_analysis._get_provider(),
    }

    context.log.info(
        f"Backtest investment recommendations generation complete: {result_metadata}"
    )
    return dg.MaterializeResult(metadata=result_metadata)
