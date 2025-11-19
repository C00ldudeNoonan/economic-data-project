from typing import Optional
from datetime import datetime
import dagster as dg

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.agents.economy_state_analyzer import EconomicAnalysisResource
from macro_agents.defs.agents.asset_class_relationship_analyzer import (
    AssetClassRelationshipModule,
    extract_relationship_summary,
)
from macro_agents.defs.agents.backtest_economy_state_analyzer import (
    BacktestConfig,
    backtest_analyze_economy_state,
)


def get_latest_backtest_economy_state_analysis(
    md_resource: MotherDuckResource,
    backtest_date: str,
    model_name: str,
) -> Optional[str]:
    """Get the latest backtest economy state analysis from the database."""
    query = f"""
    SELECT analysis_content
    FROM backtest_economy_state_analysis
    WHERE analysis_type = 'economy_state'
        AND backtest_date = '{backtest_date}'
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
) -> dg.MaterializeResult:
    """
    Asset that analyzes relationships between asset classes for backtesting.

    This is the backtest version of analyze_asset_class_relationships.

    Returns:
        Dictionary with analysis metadata and results
    """
    context.log.info(
        f"Starting backtest asset class relationship analysis for {config.backtest_date} "
        f"with model {config.model_name}..."
    )

    context.log.info("Retrieving backtest economy state analysis...")
    economy_state_analysis = get_latest_backtest_economy_state_analysis(
        md, config.backtest_date, config.model_name
    )

    if not economy_state_analysis:
        raise ValueError(
            f"No backtest economy state analysis found for {config.backtest_date} "
            f"with model {config.model_name}. Please run backtest_analyze_economy_state first."
        )

    context.log.info("Gathering market performance data with cutoff date...")
    market_data = economic_analysis.get_market_data(
        md, cutoff_date=config.backtest_date
    )

    context.log.info("Gathering correlation data with cutoff date...")
    try:
        correlation_data = economic_analysis.get_correlation_data(
            md,
            sample_size=100,
            sampling_strategy="top_correlations",
            cutoff_date=config.backtest_date,
        )
    except Exception as e:
        context.log.warning(
            f"Could not retrieve correlation data: {e}. Continuing without it."
        )
        correlation_data = ""

    context.log.info("Gathering commodity data with cutoff date...")
    commodity_data = economic_analysis.get_commodity_data(
        md, cutoff_date=config.backtest_date
    )

    relationship_analyzer = AssetClassRelationshipModule()

    context.log.info(
        "Running asset class relationship analysis with historical data..."
    )
    analysis_result = relationship_analyzer(
        economy_state_analysis=economy_state_analysis,
        market_data=market_data,
        correlation_data=correlation_data,
        commodity_data=commodity_data,
    )

    analysis_timestamp = datetime.now()
    result = {
        "analysis_timestamp": analysis_timestamp.isoformat(),
        "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
        "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
        "backtest_date": config.backtest_date,
        "model_name": config.model_name,
        "analysis_content": analysis_result.relationship_analysis,
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
    }

    json_result = {
        "analysis_type": "asset_class_relationships",
        "analysis_content": result["analysis_content"],
        "analysis_timestamp": result["analysis_timestamp"],
        "analysis_date": result["analysis_date"],
        "analysis_time": result["analysis_time"],
        "backtest_date": result["backtest_date"],
        "model_name": result["model_name"],
        "data_sources": result["data_sources"],
        "dagster_run_id": context.run_id,
        "dagster_asset_key": str(context.asset_key),
    }

    context.log.info(
        "Writing backtest asset class relationship analysis to database..."
    )
    md.write_results_to_table(
        [json_result],
        output_table="backtest_asset_class_relationship_analysis",
        if_exists="append",
        context=context,
    )

    analysis_summary = extract_relationship_summary(result["analysis_content"])

    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": result["analysis_timestamp"],
        "backtest_date": result["backtest_date"],
        "model_name": result["model_name"],
        "output_table": "backtest_asset_class_relationship_analysis",
        "records_written": 1,
        "data_sources": result["data_sources"],
        "analysis_summary": analysis_summary,
        "analysis_preview": result["analysis_content"][:500]
        if result["analysis_content"]
        else "",
    }

    context.log.info(
        f"Backtest asset class relationship analysis complete: {result_metadata}"
    )
    return dg.MaterializeResult(metadata=result_metadata)
