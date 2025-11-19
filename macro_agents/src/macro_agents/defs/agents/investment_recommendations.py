import dspy
from typing import Optional, Dict, Any
from datetime import datetime
import dagster as dg
import re

from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.agents.economy_state_analyzer import (
    analyze_economy_state,
    EconomicAnalysisResource,
    EconomicAnalysisConfig,
)
from macro_agents.defs.agents.asset_class_relationship_analyzer import (
    analyze_asset_class_relationships,
)


class InvestmentRecommendationsSignature(dspy.Signature):
    """Generate actionable investment recommendations based on economy state and asset class relationships."""

    economy_state_analysis: str = dspy.InputField(
        desc="Analysis of current economic state and cycle position from Step 1"
    )

    asset_class_relationship_analysis: str = dspy.InputField(
        desc="Analysis of relationships between asset classes and economic cycle from Step 2"
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical' (default, bearish, defensive positioning), 'neutral' (balanced, market-weight), or 'bullish' (optimistic, growth-oriented positioning)"
    )

    recommendations: str = dspy.OutputField(
        desc="""Comprehensive investment recommendations reflecting the specified personality perspective, including:
        1. Asset Allocation Recommendations:
           - OVERWEIGHT: Specific asset classes/sectors to overweight with rationale, confidence level (0-1), and expected return
           - NEUTRAL: Asset classes/sectors to hold at market weight with rationale
           - UNDERWEIGHT: Asset classes/sectors to underweight or short with rationale, confidence level, and expected return
           - Provide specific symbols/ETFs where applicable (e.g., XLK, SPY, XLE, etc.)
        2. Portfolio Allocation by Asset Class:
           - Equities: % allocation with sector breakdown and specific recommendations
           - Fixed Income: % allocation with duration/credit quality guidance
           - Alternatives: % allocation (REITs, commodities, etc.)
           - Cash: % allocation and rationale
        3. Sector Recommendations:
           - Detailed sector-by-sector analysis with specific ETF recommendations
           - Rationale based on economic cycle phase and historical patterns
           - Confidence levels and expected returns for each sector
        4. Geographic Allocation:
           - Domestic vs International allocation recommendations
           - Regional preferences (US, Europe, Asia, Emerging Markets)
           - Rationale based on economic conditions
        5. Risk Assessment:
           - Key risks to monitor based on current economic state
           - Potential inflection points that could change recommendations
           - Risk levels for each recommended position
           - Position sizing guidelines
           - Stop-loss levels where applicable
           - Hedging strategies if needed
        6. Time Horizon:
           - Expected duration of current cycle phase
           - Recommended holding periods for different positions
           - When to reassess and rebalance
        7. Implementation Timeline:
           - Immediate actions (next 30 days)
           - Medium-term adjustments (3-6 months)
           - Long-term strategic changes (6+ months)
        8. Monitoring and Rebalancing:
           - Key indicators to monitor
           - Rebalancing triggers
           - Conditions that would warrant changing recommendations
        
        Personality Guidelines:
        - SKEPTICAL/BEARISH: Favor defensive assets (utilities, consumer staples, bonds, cash). Underweight growth/cyclical sectors. Emphasize capital preservation. Higher cash allocation. Focus on downside protection and hedging.
        - NEUTRAL: Balanced allocation across asset classes. Market-weight positioning. Moderate risk exposure. Diversified approach without extreme tilts.
        - BULLISH/HOPEFUL: Favor growth assets (technology, consumer discretionary, small caps). Overweight equities vs bonds. Lower cash allocation. Focus on upside capture and growth opportunities. More aggressive positioning.
        
        Provide specific, actionable recommendations with quantitative guidance where possible. Use structured format with clear categories and confidence levels."""
    )


class InvestmentRecommendationsModule(dspy.Module):
    """DSPy module for generating investment recommendations."""

    def __init__(self, personality: str = "skeptical"):
        super().__init__()
        self.personality = personality
        self.generate_recommendations = dspy.ChainOfThought(
            InvestmentRecommendationsSignature
        )

    def forward(
        self,
        economy_state_analysis: str,
        asset_class_relationship_analysis: str,
        personality: str = None,
    ):
        personality_to_use = personality or self.personality
        return self.generate_recommendations(
            economy_state_analysis=economy_state_analysis,
            asset_class_relationship_analysis=asset_class_relationship_analysis,
            personality=personality_to_use,
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


def get_latest_relationship_analysis(
    md_resource: MotherDuckResource,
) -> Optional[str]:
    """Get the latest asset class relationship analysis from the database."""
    query = """
    SELECT analysis_content
    FROM asset_class_relationship_analysis
    WHERE analysis_type = 'asset_class_relationships'
    ORDER BY analysis_timestamp DESC
    LIMIT 1
    """

    df = md_resource.execute_query(query, read_only=True)
    if df.is_empty():
        return None

    return df[0, "analysis_content"]


def extract_recommendations_summary(recommendations_content: str) -> Dict[str, Any]:
    """Extract key insights from investment recommendations for metadata."""
    summary = {}

    outlook_match = re.search(
        r"(?:market outlook|outlook)[:\s]+(bullish|bearish|neutral|positive|negative)",
        recommendations_content,
        re.IGNORECASE,
    )
    if outlook_match:
        summary["market_outlook"] = outlook_match.group(1).lower()

    overweight_matches = re.findall(
        r"(?:OVERWEIGHT|overweight)[:\s]+([^.\n]+)",
        recommendations_content,
        re.IGNORECASE,
    )
    if overweight_matches:
        summary["overweight_assets"] = [m.strip() for m in overweight_matches[:5]]

    underweight_matches = re.findall(
        r"(?:UNDERWEIGHT|underweight)[:\s]+([^.\n]+)",
        recommendations_content,
        re.IGNORECASE,
    )
    if underweight_matches:
        summary["underweight_assets"] = [m.strip() for m in underweight_matches[:5]]

    allocation_match = re.search(
        r"(?:Equities|Stocks)[:\s]+(\d+)%", recommendations_content, re.IGNORECASE
    )
    if allocation_match:
        summary["equity_allocation_pct"] = int(allocation_match.group(1))

    bonds_match = re.search(
        r"(?:Fixed Income|Bonds)[:\s]+(\d+)%", recommendations_content, re.IGNORECASE
    )
    if bonds_match:
        summary["bonds_allocation_pct"] = int(bonds_match.group(1))

    cash_match = re.search(r"Cash[:\s]+(\d+)%", recommendations_content, re.IGNORECASE)
    if cash_match:
        summary["cash_allocation_pct"] = int(cash_match.group(1))

    horizon_match = re.search(
        r"(?:Time Horizon|horizon)[:\s]+([^.\n]+)",
        recommendations_content,
        re.IGNORECASE,
    )
    if horizon_match:
        summary["time_horizon"] = horizon_match.group(1).strip()

    risks_match = re.search(
        r"(?:Key Risks|Risk Assessment|Risks):\s*([^0-9]+?)(?:\d+\.|$)",
        recommendations_content,
        re.IGNORECASE | re.DOTALL,
    )
    if risks_match:
        summary["key_risks"] = risks_match.group(1)[:300].strip()

    summary["total_overweight_count"] = (
        len(overweight_matches) if overweight_matches else 0
    )
    summary["total_underweight_count"] = (
        len(underweight_matches) if underweight_matches else 0
    )

    return summary


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="economic_analysis",
    description="Generate actionable investment recommendations based on economy state and asset class relationships",
    deps=[analyze_economy_state, analyze_asset_class_relationships],
)
def generate_investment_recommendations(
    context: dg.AssetExecutionContext,
    config: EconomicAnalysisConfig,
    md: MotherDuckResource,
    economic_analysis: EconomicAnalysisResource,
) -> dg.MaterializeResult:
    """
    Asset that generates actionable investment recommendations.

    This is Step 3 of the economic analysis pipeline. It combines:
    - Economy state analysis from Step 1
    - Asset class relationship analysis from Step 2

    It provides specific, actionable investment recommendations including
    asset allocation, sector recommendations, risk assessment, and implementation guidance.

    Returns:
        Dictionary with recommendations metadata and results
    """
    context.log.info(
        f"Starting investment recommendations generation (personality: {config.personality})..."
    )

    context.log.info("Retrieving latest economy state analysis...")
    economy_state_analysis = get_latest_economy_state_analysis(md)

    if not economy_state_analysis:
        raise ValueError(
            "No economy state analysis found. Please run analyze_economy_state first."
        )

    context.log.info("Retrieving latest asset class relationship analysis...")
    relationship_analysis = get_latest_relationship_analysis(md)

    if not relationship_analysis:
        raise ValueError(
            "No asset class relationship analysis found. Please run analyze_asset_class_relationships first."
        )

    recommendations_generator = InvestmentRecommendationsModule(
        personality=config.personality
    )

    context.log.info(
        f"Generating investment recommendations (personality: {config.personality})..."
    )
    recommendations_result = recommendations_generator(
        economy_state_analysis=economy_state_analysis,
        asset_class_relationship_analysis=relationship_analysis,
        personality=config.personality,
    )

    analysis_timestamp = datetime.now()
    result = {
        "analysis_timestamp": analysis_timestamp.isoformat(),
        "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
        "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
        "model_name": economic_analysis.model_name,
        "personality": config.personality,
        "recommendations_content": recommendations_result.recommendations,
        "data_sources": {
            "economy_state_table": "economy_state_analysis",
            "relationship_analysis_table": "asset_class_relationship_analysis",
        },
    }

    json_result = {
        "analysis_type": "investment_recommendations",
        "recommendations_content": result["recommendations_content"],
        "analysis_timestamp": result["analysis_timestamp"],
        "analysis_date": result["analysis_date"],
        "analysis_time": result["analysis_time"],
        "model_name": result["model_name"],
        "personality": result["personality"],
        "data_sources": result["data_sources"],
        "dagster_run_id": context.run_id,
        "dagster_asset_key": str(context.asset_key),
    }

    context.log.info("Writing investment recommendations to database...")
    md.write_results_to_table(
        [json_result],
        output_table="investment_recommendations",
        if_exists="append",
        context=context,
    )

    recommendations_summary = extract_recommendations_summary(
        result["recommendations_content"]
    )

    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": result["analysis_timestamp"],
        "model_name": result["model_name"],
        "personality": result["personality"],
        "output_table": "investment_recommendations",
        "records_written": 1,
        "data_sources": result["data_sources"],
        "recommendations_summary": recommendations_summary,
        "recommendations_preview": result["recommendations_content"][:500]
        if result["recommendations_content"]
        else "",
    }

    context.log.info(
        f"Investment recommendations generation complete: {result_metadata}"
    )
    return dg.MaterializeResult(metadata=result_metadata)
