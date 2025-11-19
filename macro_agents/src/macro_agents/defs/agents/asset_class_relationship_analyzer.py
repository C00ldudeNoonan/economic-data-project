import dspy
from typing import Optional, Dict, Any
from datetime import datetime
import dagster as dg
import re

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.agents.economy_state_analyzer import (
    analyze_economy_state,
    EconomicAnalysisResource,
)


class AssetClassRelationshipSignature(dspy.Signature):
    """Analyze relationships between asset classes and the economic cycle."""

    economy_state_analysis: str = dspy.InputField(
        desc="Analysis of current economic state and cycle position from Step 1"
    )

    market_data: str = dspy.InputField(
        desc="CSV data containing recent market performance across different asset classes and sectors"
    )

    correlation_data: str = dspy.InputField(
        desc="CSV data containing correlation analysis between economic indicators and asset returns (optional, may be empty)"
    )

    commodity_data: str = dspy.InputField(
        desc="CSV data containing commodity price performance across energy, industrial/input, and agricultural commodities with returns, volatility, and trends over different time periods"
    )

    relationship_analysis: str = dspy.OutputField(
        desc="""Comprehensive analysis of asset class relationships with economic cycle including:
        1. Asset Class Performance by Economic Cycle Phase:
           - How different asset classes (equities, bonds, commodities, real estate, etc.) perform in each cycle phase
           - Historical patterns and typical returns by phase
           - Sector rotation patterns across cycle phases
           - Commodity performance patterns by cycle phase (energy, industrial, agricultural)
        2. Correlation Analysis:
           - Strongest correlations between economic indicators and asset returns
           - Leading indicators that predict asset performance
           - Temporal patterns in correlations (how relationships change over time)
           - Correlations between commodity prices and other asset classes
        3. Economic Indicator Impact on Asset Classes:
           - Which indicators most strongly influence each asset class
           - Growth vs Inflation impacts on different assets
           - Interest rate sensitivity by asset class
           - How commodity price movements relate to economic indicators
        4. Cross-Asset Relationships:
           - How different asset classes relate to each other in current cycle phase
           - Diversification benefits and correlations
           - Relative strength patterns
           - Commodity-equity relationships and commodity-bond relationships
           - How commodity categories (energy, input, agriculture) relate to sectors
        5. Commodity-Specific Analysis:
           - Energy commodity relationships with economic cycle and other assets
           - Industrial/input commodity relationships (metals, materials) with manufacturing and sectors
           - Agricultural commodity relationships with inflation and consumer sectors
           - Commodity price momentum as signals for other asset classes
           - Supply chain impacts reflected in commodity-asset relationships
        6. Category-Specific Insights:
           - Most predictive indicators by asset category (sectors, fixed income, commodities, etc.)
           - Unexpected correlations or anomalies
           - Data quality considerations
           - Commodity category-specific patterns and relationships
        7. Investment Implications:
           - Reliable signals for asset allocation
           - Timing considerations based on cycle phase
           - Risk factors and relationship stability
           - Commodity allocation signals based on economic cycle and relationships
        
        Focus on quantitative relationships, historical patterns, and actionable insights about how asset classes including commodities behave in different economic conditions."""
    )


class AssetClassRelationshipModule(dspy.Module):
    """DSPy module for analyzing asset class relationships with economic cycle."""

    def __init__(self):
        super().__init__()
        self.analyze_relationships = dspy.ChainOfThought(
            AssetClassRelationshipSignature
        )

    def forward(
        self,
        economy_state_analysis: str,
        market_data: str,
        correlation_data: str = "",
        commodity_data: str = "",
    ):
        return self.analyze_relationships(
            economy_state_analysis=economy_state_analysis,
            market_data=market_data,
            correlation_data=correlation_data
            if correlation_data
            else "No correlation data available.",
            commodity_data=commodity_data
            if commodity_data
            else "No commodity data available.",
        )


def get_latest_economy_state_analysis(
    md_resource: MotherDuckResource,
) -> Optional[str]:
    """Get the latest economy state analysis from the database."""
    query = """
    SELECT analysis_content
    FROM economy_state_analysis
    WHERE analysis_type = 'economy_state'
    ORDER BY analysis_timestamp DESC
    LIMIT 1
    """

    df = md_resource.execute_query(query, read_only=True)
    if df.is_empty():
        return None

    return df[0, "analysis_content"]


def extract_relationship_summary(analysis_content: str) -> Dict[str, Any]:
    """Extract key insights from asset class relationship analysis for metadata."""
    summary = {}

    correlation_matches = re.findall(
        r"(?:strongest|highest|strong).*?correlation[:\s]+([^.\n]+)",
        analysis_content,
        re.IGNORECASE,
    )
    if correlation_matches:
        summary["key_correlations"] = correlation_matches[:3]

    indicator_matches = re.findall(
        r"(?:leading indicator|predictive indicator)[:\s]+([^.\n]+)",
        analysis_content,
        re.IGNORECASE,
    )
    if indicator_matches:
        summary["leading_indicators"] = indicator_matches[:5]

    phase_performance = re.search(
        r"(?:Asset Class Performance|Performance by Phase).*?([^0-9]+?)(?:\d+\.|$)",
        analysis_content,
        re.IGNORECASE | re.DOTALL,
    )
    if phase_performance:
        summary["phase_performance_summary"] = phase_performance.group(1)[:500].strip()

    implications = re.search(
        r"(?:Investment Implications|Implications):\s*([^0-9]+?)(?:\d+\.|$)",
        analysis_content,
        re.IGNORECASE | re.DOTALL,
    )
    if implications:
        summary["investment_implications"] = implications.group(1)[:500].strip()

    asset_classes = ["equities", "bonds", "commodities", "real estate", "cash"]
    mentioned_assets = [
        asset for asset in asset_classes if asset.lower() in analysis_content.lower()
    ]
    if mentioned_assets:
        summary["asset_classes_analyzed"] = mentioned_assets

    return summary


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="economic_analysis",
    description="Understand relationships between asset classes and the economic cycle",
    deps=[
        analyze_economy_state,
        dg.AssetKey(["us_sector_summary"]),
        dg.AssetKey(["leading_econ_return_indicator"]),
        dg.AssetKey(["energy_commodities_summary"]),
        dg.AssetKey(["input_commodities_summary"]),
        dg.AssetKey(["agriculture_commodities_summary"]),
    ],
)
def analyze_asset_class_relationships(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    economic_analysis: EconomicAnalysisResource,
) -> dg.MaterializeResult:
    """
    Asset that analyzes relationships between asset classes and the economic cycle.

    This is Step 2 of the economic analysis pipeline. It combines:
    - Economy state analysis from Step 1
    - Market performance data
    - Correlation data between economic indicators and asset returns
    - Commodity price performance data

    It provides insights into how different asset classes including commodities perform in different
    economic cycle phases and identifies key relationships.

    Returns:
        Dictionary with analysis metadata and results
    """
    context.log.info("Starting asset class relationship analysis...")

    context.log.info("Retrieving latest economy state analysis...")
    economy_state_analysis = get_latest_economy_state_analysis(md)

    if not economy_state_analysis:
        raise ValueError(
            "No economy state analysis found. Please run analyze_economy_state first."
        )

    context.log.info("Gathering market performance data...")
    market_data = economic_analysis.get_market_data(md)

    context.log.info("Gathering correlation data...")
    try:
        correlation_data = economic_analysis.get_correlation_data(
            md, sample_size=100, sampling_strategy="top_correlations"
        )
    except Exception as e:
        context.log.warning(
            f"Could not retrieve correlation data: {e}. Continuing without it."
        )
        correlation_data = ""

    context.log.info("Gathering commodity data...")
    commodity_data = economic_analysis.get_commodity_data(md)

    relationship_analyzer = AssetClassRelationshipModule()

    context.log.info(
        "Running asset class relationship analysis with market and commodity data..."
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
        "model_name": economic_analysis.model_name,
        "analysis_content": analysis_result.relationship_analysis,
        "data_sources": {
            "economy_state_table": "economy_state_analysis",
            "market_data_table": "us_sector_summary",
            "correlation_data_table": "leading_econ_return_indicator",
            "commodity_data_tables": [
                "energy_commodities_summary",
                "input_commodities_summary",
                "agriculture_commodities_summary",
            ],
        },
    }

    json_result = {
        "analysis_type": "asset_class_relationships",
        "analysis_content": result["analysis_content"],
        "analysis_timestamp": result["analysis_timestamp"],
        "analysis_date": result["analysis_date"],
        "analysis_time": result["analysis_time"],
        "model_name": result["model_name"],
        "data_sources": result["data_sources"],
        "dagster_run_id": context.run_id,
        "dagster_asset_key": str(context.asset_key),
    }

    context.log.info("Writing asset class relationship analysis to database...")
    md.write_results_to_table(
        [json_result],
        output_table="asset_class_relationship_analysis",
        if_exists="append",
        context=context,
    )

    analysis_summary = extract_relationship_summary(result["analysis_content"])

    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": result["analysis_timestamp"],
        "model_name": result["model_name"],
        "output_table": "asset_class_relationship_analysis",
        "records_written": 1,
        "data_sources": result["data_sources"],
        "analysis_summary": analysis_summary,
        "analysis_preview": result["analysis_content"][:500]
        if result["analysis_content"]
        else "",
    }

    context.log.info(f"Asset class relationship analysis complete: {result_metadata}")
    return dg.MaterializeResult(metadata=result_metadata)
