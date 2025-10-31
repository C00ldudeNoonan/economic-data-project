import dspy
from typing import Optional, Dict, Any, List
import json
from datetime import datetime
import dagster as dg
from pydantic import Field

from macro_agents.defs.resources.motherduck import MotherDuckResource


class EnhancedEconomicCycleAnalysisSignature(dspy.Signature):
    """Enhanced economic cycle analysis with structured output for better evaluation."""

    economic_data: str = dspy.InputField(
        desc="CSV data containing latest economic indicators with current values and percentage changes over 3m, 6m, 1y periods"
    )

    market_data: str = dspy.InputField(
        desc="CSV data containing recent market performance across different asset classes and sectors"
    )

    analysis: str = dspy.OutputField(
        desc="""Structured economic cycle analysis including:
        1. Current Economic Cycle Position (Early/Expansion/Late/Recession with confidence level 0-1)
        2. Key Economic Indicators Analysis with specific metrics
        3. Market Performance Context with quantitative measures
        4. Asset Allocation Recommendations in structured format:
           - OVERWEIGHT: [{"symbol": "XLK", "confidence": 0.8, "expected_return": 0.05, "rationale": "..."}]
           - NEUTRAL: [{"symbol": "SPY", "confidence": 0.6, "expected_return": 0.02, "rationale": "..."}]
           - UNDERWEIGHT: [{"symbol": "XLE", "confidence": 0.7, "expected_return": -0.03, "rationale": "..."}]
        5. Risk Assessment with specific risk levels
        6. Time Horizon with confidence intervals
        
        Provide quantitative metrics and structured JSON where possible."""
    )


class EnhancedMarketTrendAnalysisSignature(dspy.Signature):
    """Enhanced market trend analysis with structured output."""

    market_performance: str = dspy.InputField(
        desc="CSV data containing detailed market performance metrics across sectors and asset classes"
    )

    economic_context: str = dspy.InputField(
        desc="Economic cycle analysis results providing context for market interpretation"
    )

    trend_analysis: str = dspy.OutputField(
        desc="""Structured market trend analysis including:
        1. Momentum Analysis with quantitative scores
        2. Inflection Point Detection with probability estimates
        3. Sector Rotation Patterns with specific recommendations
        4. Volatility Analysis with risk metrics
        5. Relative Strength Analysis with rankings
        6. Market Breadth Analysis with participation metrics
        7. Technical Indicators with specific values
        8. Risk-Adjusted Performance with Sharpe ratios
        
        Provide structured data and quantitative metrics where possible."""
    )


class EnhancedEconomicCycleModule(dspy.Module):
    """Enhanced economic cycle module with better structure for evaluation."""

    def __init__(self):
        super().__init__()
        self.analyze_cycle = dspy.ChainOfThought(EnhancedEconomicCycleAnalysisSignature)

    def forward(self, economic_data: str, market_data: str):
        return self.analyze_cycle(economic_data=economic_data, market_data=market_data)


class EnhancedMarketTrendModule(dspy.Module):
    """Enhanced market trend module with better structure for evaluation."""

    def __init__(self):
        super().__init__()
        self.analyze_trends = dspy.ChainOfThought(EnhancedMarketTrendAnalysisSignature)

    def forward(self, market_performance: str, economic_context: str):
        return self.analyze_trends(
            market_performance=market_performance, economic_context=economic_context
        )


class EnhancedEconomicCycleAnalyzer(dg.ConfigurableResource):
    """Enhanced economic cycle analyzer with DSPy evaluation integration."""

    model_name: str = Field(
        default="gpt-4-turbo-preview", description="LLM model to use for analysis"
    )
    openai_api_key: str = Field(description="OpenAI API key for DSPy")
    use_optimized_prompts: bool = Field(
        default=False, description="Whether to use optimized prompts from evaluation"
    )

    def setup_for_execution(self, context) -> None:
        """Initialize DSPy when the resource is used."""
        # Initialize DSPy
        lm = dspy.LM(model=self.model_name, api_key=self.openai_api_key)
        dspy.settings.configure(lm=lm)

        # Initialize analyzers
        self._cycle_analyzer = EnhancedEconomicCycleModule()
        self._trend_analyzer = EnhancedMarketTrendModule()

        # Load optimized prompts if available
        if self.use_optimized_prompts:
            self._load_optimized_prompts()

    def _load_optimized_prompts(self):
        """Load optimized prompts from evaluation results."""
        # This would load the optimized prompts from the database
        # For now, we'll use the default prompts
        pass

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
        context: Optional[dg.AssetExecutionContext] = None,
    ) -> Dict[str, Any]:
        """Analyze current economic cycle position and provide recommendations."""
        if context:
            context.log.info("Gathering economic data...")

        # Get economic and market data
        economic_data = self.get_economic_data(md_resource)
        market_data = self.get_market_data(md_resource)

        if context:
            context.log.info("Running enhanced economic cycle analysis...")

        # Run cycle analysis
        cycle_result = self.cycle_analyzer(
            economic_data=economic_data, market_data=market_data
        )

        # Run trend analysis with economic context
        if context:
            context.log.info("Running enhanced market trend analysis...")

        trend_result = self.trend_analyzer(
            market_performance=market_data, economic_context=cycle_result.analysis
        )

        # Parse structured recommendations
        structured_recommendations = self._parse_structured_recommendations(
            cycle_result.analysis
        )

        # Format results
        analysis_timestamp = datetime.now()
        result = {
            "analysis_timestamp": analysis_timestamp.isoformat(),
            "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
            "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
            "model_name": self.model_name,
            "use_optimized_prompts": self.use_optimized_prompts,
            "economic_cycle_analysis": cycle_result.analysis,
            "market_trend_analysis": trend_result.trend_analysis,
            "structured_recommendations": structured_recommendations,
            "data_sources": {
                "economic_data_table": "fred_series_latest_aggregates",
                "market_data_table": "us_sector_summary",
            },
        }

        return result

    def _parse_structured_recommendations(self, analysis_text: str) -> Dict[str, Any]:
        """Parse structured recommendations from analysis text."""
        recommendations = {
            "overweight_assets": [],
            "neutral_assets": [],
            "underweight_assets": [],
            "market_outlook": "neutral",
            "confidence_score": 0.5,
            "key_risks": [],
        }

        try:
            # Try to extract JSON from the analysis text
            import re

            # Look for JSON patterns in the analysis
            json_pattern = r'\{[^{}]*"overweight_assets"[^{}]*\}'
            json_matches = re.findall(json_pattern, analysis_text, re.DOTALL)

            if json_matches:
                # Try to parse the JSON
                for match in json_matches:
                    try:
                        parsed = json.loads(match)
                        recommendations.update(parsed)
                        break
                    except json.JSONDecodeError:
                        continue

            # Fallback: extract using regex patterns
            if not recommendations["overweight_assets"]:
                recommendations = self._extract_recommendations_regex(analysis_text)

        except Exception:
            # Fallback to regex extraction
            recommendations = self._extract_recommendations_regex(analysis_text)

        return recommendations

    def _extract_recommendations_regex(self, analysis_text: str) -> Dict[str, Any]:
        """Extract recommendations using regex patterns as fallback."""
        recommendations = {
            "overweight_assets": [],
            "neutral_assets": [],
            "underweight_assets": [],
            "market_outlook": "neutral",
            "confidence_score": 0.5,
            "key_risks": [],
        }

        # Common asset symbols
        asset_symbols = [
            "XLK",
            "XLC",
            "XLY",
            "XLF",
            "XLI",
            "XLU",
            "XLP",
            "XLRE",
            "XLB",
            "XLE",
            "XLV",
            "SPY",
            "QQQ",
            "DIA",
            "IWM",
            "VIX",
            "CWB",
            "HYG",
            "LQD",
            "TIP",
            "GOVT",
            "MUB",
            "FXE",
            "FXY",
            "FXB",
            "FXC",
            "FXA",
            "CEW",
            "ETHE",
            "IBIT",
        ]

        # Extract OVERWEIGHT recommendations
        overweight_pattern = r"(?:OVERWEIGHT|LONG):\s*([^.\n]+)"
        overweight_matches = re.findall(
            overweight_pattern, analysis_text, re.IGNORECASE
        )

        for match in overweight_matches:
            symbols_found = [symbol for symbol in asset_symbols if symbol in match]
            for symbol in symbols_found:
                recommendations["overweight_assets"].append(
                    {
                        "symbol": symbol,
                        "confidence": 0.7,
                        "expected_return": 0.03,
                        "rationale": match.strip(),
                    }
                )

        # Extract UNDERWEIGHT recommendations
        underweight_pattern = r"(?:UNDERWEIGHT|SHORT):\s*([^.\n]+)"
        underweight_matches = re.findall(
            underweight_pattern, analysis_text, re.IGNORECASE
        )

        for match in underweight_matches:
            symbols_found = [symbol for symbol in asset_symbols if symbol in match]
            for symbol in symbols_found:
                recommendations["underweight_assets"].append(
                    {
                        "symbol": symbol,
                        "confidence": 0.7,
                        "expected_return": -0.02,
                        "rationale": match.strip(),
                    }
                )

        # Extract market outlook
        if re.search(r"(?:bullish|positive|optimistic)", analysis_text, re.IGNORECASE):
            recommendations["market_outlook"] = "bullish"
        elif re.search(
            r"(?:bearish|negative|pessimistic)", analysis_text, re.IGNORECASE
        ):
            recommendations["market_outlook"] = "bearish"

        # Extract confidence score
        confidence_match = re.search(
            r"confidence[:\s]+(\d+(?:\.\d+)?)", analysis_text, re.IGNORECASE
        )
        if confidence_match:
            recommendations["confidence_score"] = float(confidence_match.group(1))

        return recommendations

    def format_cycle_analysis_as_json(
        self, analysis_result: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Format cycle analysis results as JSON records."""
        json_results = []

        # Create separate records for cycle analysis and trend analysis
        cycle_record = {
            "analysis_type": "enhanced_economic_cycle",
            "analysis_content": analysis_result["economic_cycle_analysis"],
            "structured_recommendations": json.dumps(
                analysis_result["structured_recommendations"]
            ),
            "analysis_timestamp": analysis_result["analysis_timestamp"],
            "analysis_date": analysis_result["analysis_date"],
            "analysis_time": analysis_result["analysis_time"],
            "model_name": analysis_result["model_name"],
            "use_optimized_prompts": analysis_result["use_optimized_prompts"],
            "data_sources": analysis_result["data_sources"],
        }

        trend_record = {
            "analysis_type": "enhanced_market_trends",
            "analysis_content": analysis_result["market_trend_analysis"],
            "analysis_timestamp": analysis_result["analysis_timestamp"],
            "analysis_date": analysis_result["analysis_date"],
            "analysis_time": analysis_result["analysis_time"],
            "model_name": analysis_result["model_name"],
            "use_optimized_prompts": analysis_result["use_optimized_prompts"],
            "data_sources": analysis_result["data_sources"],
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
        output_table: str = "enhanced_economic_cycle_analysis",
        if_exists: str = "append",
        context: Optional[dg.AssetExecutionContext] = None,
    ) -> None:
        """Write cycle analysis results to database."""
        # Format results as JSON
        json_results = self.format_cycle_analysis_as_json(
            analysis_result,
            metadata={
                "dagster_run_id": context.run_id if context else None,
                "dagster_asset_key": str(context.asset_key) if context else None,
            },
        )

        # Write to database
        md_resource.write_results_to_table(
            json_results,
            output_table=output_table,
            if_exists=if_exists,
            context=context,
        )


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="analysis",
    description="Enhanced economic cycle analysis with structured output for evaluation",
)
def enhanced_economic_cycle_analysis(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
    enhanced_cycle_analyzer: EnhancedEconomicCycleAnalyzer,
) -> Dict[str, Any]:
    """
    Asset that performs enhanced economic cycle analysis with structured output.

    This enhanced version provides structured recommendations that can be
    easily evaluated and optimized using DSPy's evaluation framework.

    Returns:
        Dictionary with enhanced analysis metadata and structured recommendations
    """
    context.log.info("Starting enhanced economic cycle analysis...")

    # Run comprehensive analysis
    analysis_result = enhanced_cycle_analyzer.analyze_economic_cycle(
        md_resource=md, context=context
    )

    # Write results to database
    context.log.info("Writing enhanced cycle analysis results to database...")
    enhanced_cycle_analyzer.write_cycle_analysis_to_table(
        md_resource=md,
        analysis_result=analysis_result,
        output_table="enhanced_economic_cycle_analysis",
        if_exists="append",
        context=context,
    )

    # Return metadata
    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": analysis_result["analysis_timestamp"],
        "model_name": analysis_result["model_name"],
        "use_optimized_prompts": analysis_result["use_optimized_prompts"],
        "structured_recommendations": analysis_result["structured_recommendations"],
        "overweight_assets_count": len(
            analysis_result["structured_recommendations"]["overweight_assets"]
        ),
        "underweight_assets_count": len(
            analysis_result["structured_recommendations"]["underweight_assets"]
        ),
        "market_outlook": analysis_result["structured_recommendations"][
            "market_outlook"
        ],
        "confidence_score": analysis_result["structured_recommendations"][
            "confidence_score"
        ],
        "output_table": "enhanced_economic_cycle_analysis",
        "records_written": 2,  # cycle analysis + trend analysis
        "data_sources": analysis_result["data_sources"],
    }

    context.log.info(f"Enhanced economic cycle analysis complete: {result_metadata}")
    return result_metadata
