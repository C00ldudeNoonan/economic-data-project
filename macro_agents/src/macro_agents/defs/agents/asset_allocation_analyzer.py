import dspy
import polars as pl
from typing import Optional, Dict, Any, List, Tuple
import json
from datetime import datetime
import dagster as dg
from pydantic import Field

from macro_agents.defs.resources.motherduck import MotherDuckResource


class AssetAllocationSignature(dspy.Signature):
    """Generate specific asset allocation recommendations based on economic cycle analysis and market trends."""
    
    economic_cycle_analysis: str = dspy.InputField(
        desc="Economic cycle analysis including current cycle position and key indicators"
    )
    
    market_trend_analysis: str = dspy.InputField(
        desc="Market trend analysis including sector performance and momentum indicators"
    )
    
    current_portfolio_context: str = dspy.InputField(
        desc="Current portfolio context and constraints (optional)"
    )
    
    allocation_recommendations: str = dspy.OutputField(
        desc="""Detailed asset allocation recommendations including:
        1. Portfolio Allocation by Asset Class:
           - Equities: % allocation with sector breakdown
           - Fixed Income: % allocation with duration/credit quality
           - Alternatives: % allocation (REITs, commodities, etc.)
           - Cash: % allocation
        2. Sector Recommendations:
           - OVERWEIGHT: Sectors to overweight with rationale
           - MARKET WEIGHT: Sectors to hold at market weight
           - UNDERWEIGHT: Sectors to underweight with rationale
        3. Geographic Allocation:
           - Domestic vs International allocation
           - Regional preferences (US, Europe, Asia, Emerging Markets)
        4. Risk Management:
           - Position sizing guidelines
           - Stop-loss levels
           - Hedging strategies
        5. Implementation Timeline:
           - Immediate actions (next 30 days)
           - Medium-term adjustments (3-6 months)
           - Long-term strategic changes (6+ months)
        6. Key Risks and Monitoring:
           - Primary risks to watch
           - Key indicators to monitor
           - Rebalancing triggers
        
        Provide specific percentages and clear rationale for each recommendation."""
    )


class AssetAllocationModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.analyze_allocation = dspy.ChainOfThought(AssetAllocationSignature)
    
    def forward(self, economic_cycle_analysis: str, market_trend_analysis: str, current_portfolio_context: str = ""):
        return self.analyze_allocation(
            economic_cycle_analysis=economic_cycle_analysis,
            market_trend_analysis=market_trend_analysis,
            current_portfolio_context=current_portfolio_context
        )


class AssetAllocationAnalyzer(dg.ConfigurableResource):
    """Asset allocation analyzer that provides specific investment recommendations."""
    
    model_name: str = Field(
        default="gpt-4-turbo-preview", description="LLM model to use for analysis"
    )
    openai_api_key: str = Field(description="OpenAI API key for DSPy")
    
    def setup_for_execution(self, context) -> None:
        """Initialize DSPy when the resource is used."""
        # Initialize DSPy
        lm = dspy.LM(model=self.model_name, api_key=self.openai_api_key)
        dspy.settings.configure(lm=lm)
        
        # Initialize analyzer
        self._allocation_analyzer = AssetAllocationModule()
    
    @property
    def allocation_analyzer(self):
        """Get asset allocation analyzer."""
        return self._allocation_analyzer

    def get_latest_analysis(self, md_resource: MotherDuckResource) -> Tuple[str, str]:
        """Get the latest economic cycle and market trend analysis from database."""
        query = """
        SELECT analysis_content, analysis_type
        FROM economic_cycle_analysis
        WHERE analysis_timestamp = (
            SELECT MAX(analysis_timestamp) 
            FROM economic_cycle_analysis
        )
        ORDER BY analysis_type
        """
        
        df = md_resource.execute_query(query, read_only=True)
        
        cycle_analysis = ""
        trend_analysis = ""
        
        for row in df.iter_rows(named=True):
            if row["analysis_type"] == "economic_cycle":
                cycle_analysis = row["analysis_content"]
            elif row["analysis_type"] == "market_trends":
                trend_analysis = row["analysis_content"]
        
        return cycle_analysis, trend_analysis

    def generate_allocation_recommendations(
        self,
        md_resource: MotherDuckResource,
        portfolio_context: str = "",
        context: Optional[dg.AssetExecutionContext] = None
    ) -> Dict[str, Any]:
        """Generate asset allocation recommendations based on latest analysis."""
        if context:
            context.log.info("Retrieving latest economic and market analysis...")
        
        # Get latest analysis
        cycle_analysis, trend_analysis = self.get_latest_analysis(md_resource)
        
        if not cycle_analysis or not trend_analysis:
            raise ValueError("No recent economic cycle or market trend analysis found. Please run economic_cycle_analysis first.")
        
        if context:
            context.log.info("Generating asset allocation recommendations...")
        
        # Generate recommendations
        allocation_result = self.allocation_analyzer(
            economic_cycle_analysis=cycle_analysis,
            market_trend_analysis=trend_analysis,
            current_portfolio_context=portfolio_context
        )
        
        # Format results
        analysis_timestamp = datetime.now()
        result = {
            "analysis_timestamp": analysis_timestamp.isoformat(),
            "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
            "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
            "model_name": self.model_name,
            "allocation_recommendations": allocation_result.allocation_recommendations,
            "portfolio_context": portfolio_context,
            "source_analysis_timestamp": self._get_latest_analysis_timestamp(md_resource)
        }
        
        return result

    def _get_latest_analysis_timestamp(self, md_resource: MotherDuckResource) -> str:
        """Get timestamp of the latest analysis used for recommendations."""
        query = """
        SELECT MAX(analysis_timestamp) as latest_timestamp
        FROM economic_cycle_analysis
        """
        
        df = md_resource.execute_query(query, read_only=True)
        return df[0, "latest_timestamp"] if not df.is_empty() else ""

    def format_allocation_as_json(
        self,
        allocation_result: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Format allocation recommendations as JSON record."""
        json_result = {
            "analysis_type": "asset_allocation_recommendations",
            "analysis_content": allocation_result["allocation_recommendations"],
            "analysis_timestamp": allocation_result["analysis_timestamp"],
            "analysis_date": allocation_result["analysis_date"],
            "analysis_time": allocation_result["analysis_time"],
            "model_name": allocation_result["model_name"],
            "portfolio_context": allocation_result["portfolio_context"],
            "source_analysis_timestamp": allocation_result["source_analysis_timestamp"]
        }
        
        # Add metadata if provided
        if metadata:
            json_result.update(metadata)
        
        return json_result

    def write_allocation_to_table(
        self,
        md_resource: MotherDuckResource,
        allocation_result: Dict[str, Any],
        output_table: str = "asset_allocation_recommendations",
        if_exists: str = "append",
        context: Optional[dg.AssetExecutionContext] = None
    ) -> None:
        """Write allocation recommendations to database."""
        # Format results as JSON
        json_result = self.format_allocation_as_json(
            allocation_result,
            metadata={
                "dagster_run_id": context.run_id if context else None,
                "dagster_asset_key": str(context.asset_key) if context else None
            }
        )
        
        # Write to database
        md_resource.write_results_to_table(
            [json_result],
            output_table=output_table,
            if_exists=if_exists,
            context=context
        )


@dg.asset(
    kinds={"dspy", "analysis", "asset_allocation"},
    description="Generate asset allocation recommendations based on economic cycle and market trend analysis",
    deps=["economic_cycle_analysis"],
)
def asset_allocation_recommendations(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    allocation_analyzer: AssetAllocationAnalyzer,
) -> Dict[str, Any]:
    """
    Asset that generates specific asset allocation recommendations based on economic analysis.
    
    Returns:
        Dictionary with allocation recommendations and metadata
    """
    context.log.info("Starting asset allocation analysis...")
    
    # Generate recommendations
    allocation_result = allocation_analyzer.generate_allocation_recommendations(
        md_resource=md,
        portfolio_context="General portfolio - no specific constraints",
        context=context
    )
    
    # Write results to database
    context.log.info("Writing allocation recommendations to database...")
    allocation_analyzer.write_allocation_to_table(
        md_resource=md,
        allocation_result=allocation_result,
        output_table="asset_allocation_recommendations",
        if_exists="append",
        context=context
    )
    
    # Return metadata
    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": allocation_result["analysis_timestamp"],
        "model_name": allocation_result["model_name"],
        "output_table": "asset_allocation_recommendations",
        "records_written": 1,
        "source_analysis_timestamp": allocation_result["source_analysis_timestamp"]
    }
    
    context.log.info(f"Asset allocation analysis complete: {result_metadata}")
    return result_metadata


@dg.asset(
    kinds={"dspy", "analysis", "custom_allocation"},
    description="Generate custom asset allocation recommendations with specific portfolio context",
    deps=["economic_cycle_analysis"],
)
def custom_asset_allocation(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    allocation_analyzer: AssetAllocationAnalyzer,
) -> Dict[str, Any]:
    """
    Asset that generates custom asset allocation recommendations with specific portfolio context.
    
    This asset can be customized with different portfolio contexts by modifying the portfolio_context parameter.
    
    Returns:
        Dictionary with custom allocation recommendations and metadata
    """
    context.log.info("Starting custom asset allocation analysis...")
    
    # Define custom portfolio context
    custom_context = """
    Portfolio Context:
    - Conservative investor approaching retirement (5-10 years)
    - Risk tolerance: Moderate to Conservative
    - Current allocation: 60% stocks, 30% bonds, 10% cash
    - Geographic preference: 70% US, 30% International
    - Sector preferences: Avoid high-volatility sectors, prefer dividend-paying stocks
    - Liquidity needs: Moderate (some funds may be needed within 2-3 years)
    - Tax considerations: Taxable account, prefer tax-efficient strategies
    """
    
    # Generate custom recommendations
    allocation_result = allocation_analyzer.generate_allocation_recommendations(
        md_resource=md,
        portfolio_context=custom_context,
        context=context
    )
    
    # Write results to database
    context.log.info("Writing custom allocation recommendations to database...")
    allocation_analyzer.write_allocation_to_table(
        md_resource=md,
        allocation_result=allocation_result,
        output_table="custom_asset_allocation_recommendations",
        if_exists="append",
        context=context
    )
    
    # Return metadata
    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": allocation_result["analysis_timestamp"],
        "model_name": allocation_result["model_name"],
        "output_table": "custom_asset_allocation_recommendations",
        "records_written": 1,
        "portfolio_context": "Conservative retirement-focused",
        "source_analysis_timestamp": allocation_result["source_analysis_timestamp"]
    }
    
    context.log.info(f"Custom asset allocation analysis complete: {result_metadata}")
    return result_metadata
