import dspy
import polars as pl
from typing import Optional, Dict, Any, List, Tuple
import json
from datetime import datetime
import dagster as dg
from pydantic import Field

from macro_agents.defs.resources.motherduck import MotherDuckResource


class EconomicCycleAnalysisSignature(dspy.Signature):
    """Analyze current economic indicators to determine the economic cycle position and provide asset allocation recommendations."""
    
    economic_data: str = dspy.InputField(
        desc="CSV data containing latest economic indicators with current values and percentage changes over 3m, 6m, 1y periods"
    )
    
    market_data: str = dspy.InputField(
        desc="CSV data containing recent market performance across different asset classes and sectors"
    )
    
    analysis: str = dspy.OutputField(
        desc="""Comprehensive economic cycle analysis including:
        1. Current Economic Cycle Position (Early/Expansion/Late/Recession with confidence level)
        2. Key Economic Indicators Analysis (GDP growth, inflation, employment, interest rates, consumer sentiment)
        3. Market Performance Context (sector rotation, asset class performance, volatility trends)
        4. Asset Allocation Recommendations:
           - LONG: Asset classes/sectors to overweight (with rationale)
           - NEUTRAL: Asset classes/sectors to hold at market weight
           - SHORT: Asset classes/sectors to underweight or short (with rationale)
        5. Risk Assessment (key risks to monitor, potential inflection points)
        6. Time Horizon (expected duration of current cycle phase)
        
        Focus on leading indicators, yield curve analysis, credit spreads, and sector rotation patterns."""
    )


class MarketTrendAnalysisSignature(dspy.Signature):
    """Analyze market trends and performance patterns to identify inflection points and momentum shifts."""
    
    market_performance: str = dspy.InputField(
        desc="CSV data containing detailed market performance metrics across sectors and asset classes"
    )
    
    economic_context: str = dspy.InputField(
        desc="Economic cycle analysis results providing context for market interpretation"
    )
    
    trend_analysis: str = dspy.OutputField(
        desc="""Comprehensive market trend analysis including:
        1. Momentum Analysis (identify accelerating/decelerating trends across sectors)
        2. Inflection Point Detection (potential reversal signals, support/resistance levels)
        3. Sector Rotation Patterns (which sectors are leading/lagging and why)
        4. Volatility Analysis (volatility trends, risk-on vs risk-off sentiment)
        5. Relative Strength Analysis (best/worst performing assets and momentum)
        6. Market Breadth Analysis (participation levels, divergence signals)
        7. Technical Indicators (moving averages, momentum oscillators, volume patterns)
        8. Risk-Adjusted Performance (Sharpe ratios, drawdowns, risk-adjusted returns)
        
        Focus on identifying early warning signals and confirming trend changes."""
    )


class EconomicCycleModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.analyze_cycle = dspy.ChainOfThought(EconomicCycleAnalysisSignature)
    
    def forward(self, economic_data: str, market_data: str):
        return self.analyze_cycle(economic_data=economic_data, market_data=market_data)


class MarketTrendModule(dspy.Module):
    def __init__(self):
        super().__init__()
        self.analyze_trends = dspy.ChainOfThought(MarketTrendAnalysisSignature)
    
    def forward(self, market_performance: str, economic_context: str):
        return self.analyze_trends(market_performance=market_performance, economic_context=economic_context)


class EconomicCycleAnalyzer(dg.ConfigurableResource):
    """Economic cycle analyzer that determines cycle position and provides asset allocation recommendations."""
    
    model_name: str = Field(
        default="gpt-4-turbo-preview", description="LLM model to use for analysis"
    )
    openai_api_key: str = Field(description="OpenAI API key for DSPy")
    
    def setup_for_execution(self, context) -> None:
        """Initialize DSPy when the resource is used."""
        # Initialize DSPy
        lm = dspy.LM(model=self.model_name, api_key=self.openai_api_key)
        dspy.settings.configure(lm=lm)
        
        # Initialize analyzers
        self._cycle_analyzer = EconomicCycleModule()
        self._trend_analyzer = MarketTrendModule()
    
    @property
    def cycle_analyzer(self):
        """Get economic cycle analyzer."""
        return self._cycle_analyzer
    
    @property
    def trend_analyzer(self):
        """Get market trend analyzer."""
        return self._trend_analyzer

    def get_economic_data(self, md_resource: MotherDuckResource) -> str:
        """Get latest economic data from FRED series."""
        query = """
        SELECT 
            series_code,
            series_name,
            month,
            current_value,
            pct_change_3m,
            pct_change_6m,
            pct_change_1y,
            date_grain
        FROM fred_series_latest_aggregates
        WHERE current_value IS NOT NULL
        ORDER BY series_name, month DESC
        """
        
        df = md_resource.execute_query(query, read_only=True)
        csv_buffer = df.write_csv()
        return csv_buffer

    def get_market_data(self, md_resource: MotherDuckResource) -> str:
        """Get latest market performance data."""
        query = """
        SELECT 
            symbol,
            asset_type,
            time_period,
            exchange,
            name,
            period_start_date,
            period_end_date,
            trading_days,
            total_return_pct,
            avg_daily_return_pct,
            volatility_pct,
            win_rate_pct,
            total_price_change,
            avg_daily_price_change,
            worst_day_change,
            best_day_change,
            positive_days,
            negative_days,
            neutral_days,
            period_start_price,
            period_end_price
        FROM us_sector_summary
        WHERE time_period IN ('12_weeks', '6_months', '1_year')
        ORDER BY asset_type, time_period, total_return_pct DESC
        """
        
        df = md_resource.execute_query(query, read_only=True)
        csv_buffer = df.write_csv()
        return csv_buffer

    def analyze_economic_cycle(
        self,
        md_resource: MotherDuckResource,
        context: Optional[dg.AssetExecutionContext] = None
    ) -> Dict[str, Any]:
        """Analyze current economic cycle position and provide recommendations."""
        if context:
            context.log.info("Gathering economic data...")
        
        # Get economic and market data
        economic_data = self.get_economic_data(md_resource)
        market_data = self.get_market_data(md_resource)
        
        if context:
            context.log.info("Running economic cycle analysis...")
        
        # Run cycle analysis
        cycle_result = self.cycle_analyzer(
            economic_data=economic_data,
            market_data=market_data
        )
        
        # Run trend analysis with economic context
        if context:
            context.log.info("Running market trend analysis...")
        
        trend_result = self.trend_analyzer(
            market_performance=market_data,
            economic_context=cycle_result.analysis
        )
        
        # Format results
        analysis_timestamp = datetime.now()
        result = {
            "analysis_timestamp": analysis_timestamp.isoformat(),
            "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
            "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
            "model_name": self.model_name,
            "economic_cycle_analysis": cycle_result.analysis,
            "market_trend_analysis": trend_result.trend_analysis,
            "data_sources": {
                "economic_data_table": "fred_series_latest_aggregates",
                "market_data_table": "us_sector_summary"
            }
        }
        
        return result

    def format_cycle_analysis_as_json(
        self,
        analysis_result: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Format cycle analysis results as JSON records."""
        json_results = []
        
        # Create separate records for cycle analysis and trend analysis
        cycle_record = {
            "analysis_type": "economic_cycle",
            "analysis_content": analysis_result["economic_cycle_analysis"],
            "analysis_timestamp": analysis_result["analysis_timestamp"],
            "analysis_date": analysis_result["analysis_date"],
            "analysis_time": analysis_result["analysis_time"],
            "model_name": analysis_result["model_name"],
            "data_sources": analysis_result["data_sources"]
        }
        
        trend_record = {
            "analysis_type": "market_trends",
            "analysis_content": analysis_result["market_trend_analysis"],
            "analysis_timestamp": analysis_result["analysis_timestamp"],
            "analysis_date": analysis_result["analysis_date"],
            "analysis_time": analysis_result["analysis_time"],
            "model_name": analysis_result["model_name"],
            "data_sources": analysis_result["data_sources"]
        }
        
        # Add metadata if provided
        if metadata:
            cycle_record.update(metadata)
            trend_record.update(metadata)
        
        json_results.extend([cycle_record, trend_record])
        return json_results

    def write_cycle_analysis_to_table(
        self,
        md_resource: MotherDuckResource,
        analysis_result: Dict[str, Any],
        output_table: str = "economic_cycle_analysis",
        if_exists: str = "append",
        context: Optional[dg.AssetExecutionContext] = None
    ) -> None:
        """Write cycle analysis results to database."""
        # Format results as JSON
        json_results = self.format_cycle_analysis_as_json(
            analysis_result,
            metadata={
                "dagster_run_id": context.run_id if context else None,
                "dagster_asset_key": str(context.asset_key) if context else None
            }
        )
        
        # Write to database
        md_resource.write_results_to_table(
            json_results,
            output_table=output_table,
            if_exists=if_exists,
            context=context
        )


@dg.asset(
    kinds={"dspy", "analysis", "economic_cycle"},
    description="Analyze current economic cycle position and provide asset allocation recommendations",
    compute_kind="dspy",
)
def economic_cycle_analysis(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    cycle_analyzer: EconomicCycleAnalyzer,
) -> Dict[str, Any]:
    """
    Asset that analyzes the current economic cycle position and provides asset allocation recommendations.
    
    Returns:
        Dictionary with analysis metadata and result counts
    """
    context.log.info("Starting economic cycle analysis...")
    
    # Run comprehensive analysis
    analysis_result = cycle_analyzer.analyze_economic_cycle(
        md_resource=md,
        context=context
    )
    
    # Write results to database
    context.log.info("Writing cycle analysis results to database...")
    cycle_analyzer.write_cycle_analysis_to_table(
        md_resource=md,
        analysis_result=analysis_result,
        output_table="economic_cycle_analysis",
        if_exists="append",
        context=context
    )
    
    # Return metadata
    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": analysis_result["analysis_timestamp"],
        "model_name": analysis_result["model_name"],
        "output_table": "economic_cycle_analysis",
        "records_written": 2,  # cycle analysis + trend analysis
        "data_sources": analysis_result["data_sources"]
    }
    
    context.log.info(f"Economic cycle analysis complete: {result_metadata}")
    return result_metadata


@dg.asset(
    kinds={"dspy", "analysis", "integrated"},
    description="Integrated economic and market analysis combining cycle position with trend analysis",
    compute_kind="dspy",
    deps=[economic_cycle_analysis],
)
def integrated_economic_analysis(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    cycle_analyzer: EconomicCycleAnalyzer,
) -> Dict[str, Any]:
    """
    Asset that provides integrated economic and market analysis with actionable recommendations.
    
    Returns:
        Dictionary with comprehensive analysis results
    """
    context.log.info("Starting integrated economic analysis...")
    
    # Get the latest cycle analysis from database
    query = """
    SELECT analysis_content, analysis_type
    FROM economic_cycle_analysis
    WHERE analysis_timestamp = (
        SELECT MAX(analysis_timestamp) 
        FROM economic_cycle_analysis
    )
    ORDER BY analysis_type
    """
    
    df = md.execute_query(query, read_only=True)
    
    if df.is_empty():
        context.log.warning("No previous cycle analysis found, running new analysis...")
        return economic_cycle_analysis(context, md, cycle_analyzer)
    
    # Combine cycle and trend analysis
    cycle_analysis = None
    trend_analysis = None
    
    for row in df.iter_rows(named=True):
        if row["analysis_type"] == "economic_cycle":
            cycle_analysis = row["analysis_content"]
        elif row["analysis_type"] == "market_trends":
            trend_analysis = row["analysis_content"]
    
    # Create integrated analysis
    integrated_result = {
        "analysis_timestamp": datetime.now().isoformat(),
        "analysis_type": "integrated_economic_analysis",
        "economic_cycle_analysis": cycle_analysis,
        "market_trend_analysis": trend_analysis,
        "integration_notes": "Combined economic cycle position with market trend analysis for comprehensive investment recommendations",
        "model_name": cycle_analyzer.model_name
    }
    
    # Write integrated analysis to database
    context.log.info("Writing integrated analysis to database...")
    md.write_results_to_table(
        [integrated_result],
        output_table="integrated_economic_analysis",
        if_exists="append",
        context=context
    )
    
    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": integrated_result["analysis_timestamp"],
        "output_table": "integrated_economic_analysis",
        "records_written": 1,
        "analysis_components": ["economic_cycle", "market_trends"]
    }
    
    context.log.info(f"Integrated economic analysis complete: {result_metadata}")
    return result_metadata
