import dspy
from typing import Dict, Any, Optional
from datetime import datetime
import dagster as dg
from pydantic import Field
import re

from macro_agents.defs.resources.motherduck import MotherDuckResource


class EconomicAnalysisConfig(dg.Config):
    """Configuration for economic analysis assets."""

    personality: str = Field(
        default="skeptical",
        description="Analytical personality: 'skeptical' (default, bearish), 'neutral' (balanced), or 'bullish' (optimistic)",
    )


class EconomyStateAnalysisSignature(dspy.Signature):
    """Analyze current economic indicators to determine the state of the economy."""

    economic_data: str = dspy.InputField(
        desc="CSV data containing latest economic indicators with current values and percentage changes over 3m, 6m, 1y periods"
    )

    commodity_data: str = dspy.InputField(
        desc="CSV data containing commodity price performance across energy, industrial/input, and agricultural commodities with returns, volatility, and trends over different time periods"
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical' (default, bearish, focuses on risks and downside), 'neutral' (balanced, objective), or 'bullish' (optimistic, focuses on opportunities and upside)"
    )

    analysis: str = dspy.OutputField(
        desc="""Comprehensive economic state analysis reflecting the specified personality perspective, including:
        1. Current Economic Cycle Position (Early/Expansion/Late/Recession with confidence level 0-1)
        2. Key Economic Indicators Analysis:
           - GDP growth trends and outlook
           - Inflation levels and trajectory (CPI, PCE, core inflation)
           - Employment metrics (unemployment rate, job growth, labor force participation)
           - Interest rates and monetary policy stance
           - Consumer sentiment and spending indicators
           - Housing market indicators
           - Manufacturing and services PMI
        3. Commodity Market Analysis:
           - Energy commodity trends (oil, gas, coal) and implications for economic activity
           - Industrial/input commodity prices (metals, materials) and manufacturing signals
           - Agricultural commodity prices and food inflation pressures
           - Commodity price trends as leading indicators of economic activity
           - Supply chain and cost pressure signals from commodity markets
        4. Leading Indicators Assessment:
           - Yield curve analysis (normal, flat, inverted)
           - Credit spreads and financial conditions
           - Business confidence indicators
           - Leading economic index components
           - Commodity price momentum as economic signals
        5. Economic Cycle Phase Characteristics:
           - Current phase identification with supporting evidence
           - Phase duration and typical progression patterns
           - Key indicators suggesting phase transitions
           - Commodity cycle alignment with economic cycle
        6. Risk Factors (skeptical: emphasize risks, neutral: balanced, bullish: acknowledge but minimize):
           - Economic imbalances or vulnerabilities
           - Potential inflection points
           - External risks (geopolitical, trade, etc.)
           - Commodity price shocks and their economic impact
        
        Personality Guidelines:
        - SKEPTICAL/BEARISH: Emphasize downside risks, vulnerabilities, potential recessions, negative indicators. Be cautious and highlight what could go wrong. Focus on defensive positioning.
        - NEUTRAL: Provide balanced, objective analysis weighing both positive and negative factors equally. Avoid extreme positions.
        - BULLISH/HOPEFUL: Emphasize opportunities, positive trends, resilience, and upside potential. Highlight strengths and growth prospects. Focus on expansionary positioning.
        
        Focus on quantitative metrics, trends, and leading indicators including commodity markets to provide a clear assessment of the current economic state from the specified perspective."""
    )


class EconomyStateModule(dspy.Module):
    """DSPy module for analyzing current economy state."""

    def __init__(self, personality: str = "skeptical"):
        super().__init__()
        self.personality = personality
        self.analyze_state = dspy.ChainOfThought(EconomyStateAnalysisSignature)

    def forward(self, economic_data: str, commodity_data: str, personality: str = None):
        personality_to_use = personality or self.personality
        return self.analyze_state(
            economic_data=economic_data,
            commodity_data=commodity_data,
            personality=personality_to_use,
        )


class EconomicAnalysisResource(dg.ConfigurableResource):
    """Unified resource for economic analysis that consolidates useful parts from existing analyzers."""

    model_name: str = Field(
        default="gpt-4-turbo-preview", description="LLM model to use for analysis"
    )
    openai_api_key: str = Field(description="OpenAI API key for DSPy")

    def setup_for_execution(self, context) -> None:
        """Initialize DSPy when the resource is used."""
        lm = dspy.LM(model=self.model_name, api_key=self.openai_api_key)
        dspy.settings.configure(lm=lm)
        self._economy_state_analyzer = EconomyStateModule()

    @property
    def economy_state_analyzer(self):
        """Get economy state analyzer."""
        return self._economy_state_analyzer

    def get_economic_data(
        self, md_resource: MotherDuckResource, cutoff_date: Optional[str] = None
    ) -> str:
        """Get latest economic data from FRED series.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
        """
        if cutoff_date:
            query = f"""
            SELECT 
                series_code,
                series_name,
                month,
                current_value,
                pct_change_3m,
                pct_change_6m,
                pct_change_1y,
                date_grain
            FROM fred_series_latest_aggregates_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND current_value IS NOT NULL
            ORDER BY series_name, month DESC
            """
        else:
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

        df = md_resource.execute_query(query)
        csv_buffer = df.write_csv()
        return csv_buffer

    def get_market_data(
        self, md_resource: MotherDuckResource, cutoff_date: Optional[str] = None
    ) -> str:
        """Get latest market performance data.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
        """
        if cutoff_date:
            query = f"""
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
            FROM us_sector_summary_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND time_period IN ('12_weeks', '6_months', '1_year')
            ORDER BY asset_type, time_period, total_return_pct DESC
            """
        else:
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

        df = md_resource.execute_query(query)
        csv_buffer = df.write_csv()
        return csv_buffer

    def get_commodity_data(
        self, md_resource: MotherDuckResource, cutoff_date: Optional[str] = None
    ) -> str:
        """Get latest commodity performance data from all commodity summary tables.

        Args:
            md_resource: MotherDuck resource
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
        """
        if cutoff_date:
            query = f"""
            SELECT 
                commodity_name,
                commodity_unit,
                'energy' as commodity_category,
                time_period,
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
            FROM energy_commodities_summary_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND time_period IN ('12_weeks', '6_months', '1_year')
            
            UNION ALL
            
            SELECT 
                commodity_name,
                commodity_unit,
                'input' as commodity_category,
                time_period,
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
            FROM input_commodities_summary_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND time_period IN ('12_weeks', '6_months', '1_year')
            
            UNION ALL
            
            SELECT 
                commodity_name,
                commodity_unit,
                'agriculture' as commodity_category,
                time_period,
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
            FROM agriculture_commodities_summary_snapshot
            WHERE snapshot_date = '{cutoff_date}'
                AND time_period IN ('12_weeks', '6_months', '1_year')
            
            ORDER BY commodity_category, commodity_name, time_period, total_return_pct DESC
            """
        else:
            query = """
            SELECT 
                commodity_name,
                commodity_unit,
                'energy' as commodity_category,
                time_period,
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
            FROM energy_commodities_summary
            WHERE time_period IN ('12_weeks', '6_months', '1_year')
            
            UNION ALL
            
            SELECT 
                commodity_name,
                commodity_unit,
                'input' as commodity_category,
                time_period,
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
            FROM input_commodities_summary
            WHERE time_period IN ('12_weeks', '6_months', '1_year')
            
            UNION ALL
            
            SELECT 
                commodity_name,
                commodity_unit,
                'agriculture' as commodity_category,
                time_period,
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
            FROM agriculture_commodities_summary
            WHERE time_period IN ('12_weeks', '6_months', '1_year')
            
            ORDER BY commodity_category, commodity_name, time_period, total_return_pct DESC
            """

        df = md_resource.execute_query(query)
        csv_buffer = df.write_csv()
        return csv_buffer

    def get_correlation_data(
        self,
        md_resource: MotherDuckResource,
        sample_size: int = 100,
        sampling_strategy: str = "top_correlations",
        cutoff_date: Optional[str] = None,
    ) -> str:
        """Get correlation data between economic indicators and asset returns.

        Args:
            md_resource: MotherDuck resource
            sample_size: Number of samples to return
            sampling_strategy: Strategy for sampling
            cutoff_date: Optional date string (YYYY-MM-DD) for backtesting.
                        If provided, uses snapshot tables.
        """
        snapshot_table = "leading_econ_return_indicator_snapshot"

        if cutoff_date:
            if md_resource.table_exists(snapshot_table):
                table_name = snapshot_table
            else:
                table_name = "leading_econ_return_indicator"
        else:
            table_name = "leading_econ_return_indicator"

        if cutoff_date:
            if hasattr(md_resource, "query_sampled_data"):
                try:
                    correlation_data = md_resource.query_sampled_data(
                        table_name=table_name,
                        filters={"snapshot_date": cutoff_date}
                        if table_name == snapshot_table
                        else {},
                        sample_size=sample_size,
                        sampling_strategy=sampling_strategy,
                    )
                    return correlation_data
                except Exception:
                    return ""
            else:
                try:
                    if table_name == snapshot_table:
                        query = f"""
                        SELECT *
                        FROM {table_name}
                        WHERE snapshot_date = '{cutoff_date}'
                        ORDER BY GREATEST(
                            ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                            ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                            ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                        ) DESC
                        LIMIT {sample_size}
                        """
                    else:
                        query = f"""
                        SELECT *
                        FROM {table_name}
                        ORDER BY GREATEST(
                            ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                            ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                            ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                        ) DESC
                        LIMIT {sample_size}
                        """
                    df = md_resource.execute_query(query)
                    return df.write_csv()
                except Exception:
                    return ""
        else:
            if hasattr(md_resource, "query_sampled_data"):
                correlation_data = md_resource.query_sampled_data(
                    table_name=table_name,
                    filters={},
                    sample_size=sample_size,
                    sampling_strategy=sampling_strategy,
                )
                return correlation_data
            else:
                query = f"""
                SELECT *
                FROM {table_name}
                ORDER BY GREATEST(
                    ABS(COALESCE(correlation_econ_vs_q1_returns, 0)),
                    ABS(COALESCE(correlation_econ_vs_q2_returns, 0)),
                    ABS(COALESCE(correlation_econ_vs_q3_returns, 0))
                ) DESC
                LIMIT {sample_size}
                """
                df = md_resource.execute_query(query)
                return df.write_csv()


def extract_economy_state_summary(analysis_content: str) -> Dict[str, Any]:
    """Extract key insights from economy state analysis for metadata."""
    summary = {}

    cycle_match = re.search(
        r"(?:Current Economic Cycle Position|Cycle Position|Economic Cycle):\s*([^.\n]+)",
        analysis_content,
        re.IGNORECASE,
    )
    if cycle_match:
        summary["economic_cycle_position"] = cycle_match.group(1).strip()

    confidence_match = re.search(
        r"confidence[:\s]+([0-9.]+)", analysis_content, re.IGNORECASE
    )
    if confidence_match:
        try:
            summary["confidence_level"] = float(confidence_match.group(1))
        except ValueError:
            pass

    risk_section = re.search(
        r"(?:Risk Factors|Key Risks|Risks):\s*([^0-9]+?)(?:\d+\.|$)",
        analysis_content,
        re.IGNORECASE | re.DOTALL,
    )
    if risk_section:
        risks_text = risk_section.group(1)[:500]
        summary["key_risks_summary"] = risks_text.strip()

    inflation_match = re.search(
        r"(?:Inflation|CPI|PCE).*?(?:trend|trajectory|level):\s*([^.\n]+)",
        analysis_content,
        re.IGNORECASE,
    )
    if inflation_match:
        summary["inflation_trend"] = inflation_match.group(1).strip()

    gdp_match = re.search(
        r"(?:GDP|growth).*?(?:trend|outlook|growth):\s*([^.\n]+)",
        analysis_content,
        re.IGNORECASE,
    )
    if gdp_match:
        summary["gdp_outlook"] = gdp_match.group(1).strip()

    return summary


@dg.asset(
    kinds={"dspy", "duckdb"},
    group_name="economic_analysis",
    description="Analyze current economic indicators and commodity data to determine the state of the economy",
    deps=[
        dg.AssetKey(["fred_series_latest_aggregates"]),
        dg.AssetKey(["leading_econ_return_indicator"]),
        dg.AssetKey(["us_sector_summary"]),
        dg.AssetKey(["energy_commodities_summary"]),
        dg.AssetKey(["input_commodities_summary"]),
        dg.AssetKey(["agriculture_commodities_summary"]),
    ],
)
def analyze_economy_state(
    context: dg.AssetExecutionContext,
    config: EconomicAnalysisConfig,
    md: MotherDuckResource,
    economic_analysis: EconomicAnalysisResource,
) -> dg.MaterializeResult:
    """
    Asset that analyzes the current state of the economy based on economic indicators.

    This is Step 1 of the economic analysis pipeline. It focuses solely on analyzing
    economic indicators to determine the current economic state and cycle position.

    Returns:
        Dictionary with analysis metadata and results
    """
    context.log.info(
        f"Starting economy state analysis (personality: {config.personality})..."
    )

    context.log.info("Gathering economic data...")
    economic_data = economic_analysis.get_economic_data(md)

    context.log.info("Gathering commodity data...")
    commodity_data = economic_analysis.get_commodity_data(md)

    context.log.info(
        f"Running economy state analysis with economic and commodity data (personality: {config.personality})..."
    )
    analysis_result = economic_analysis.economy_state_analyzer(
        economic_data=economic_data,
        commodity_data=commodity_data,
        personality=config.personality,
    )

    analysis_timestamp = datetime.now()
    result = {
        "analysis_timestamp": analysis_timestamp.isoformat(),
        "analysis_date": analysis_timestamp.strftime("%Y-%m-%d"),
        "analysis_time": analysis_timestamp.strftime("%H:%M:%S"),
        "model_name": economic_analysis.model_name,
        "personality": config.personality,
        "analysis_content": analysis_result.analysis,
        "data_sources": {
            "economic_data_table": "fred_series_latest_aggregates",
            "commodity_data_tables": [
                "energy_commodities_summary",
                "input_commodities_summary",
                "agriculture_commodities_summary",
            ],
        },
    }

    json_result = {
        "analysis_type": "economy_state",
        "analysis_content": result["analysis_content"],
        "analysis_timestamp": result["analysis_timestamp"],
        "analysis_date": result["analysis_date"],
        "analysis_time": result["analysis_time"],
        "model_name": result["model_name"],
        "personality": result["personality"],
        "data_sources": result["data_sources"],
        "dagster_run_id": context.run_id,
        "dagster_asset_key": str(context.asset_key),
    }

    context.log.info("Writing economy state analysis to database...")
    md.write_results_to_table(
        [json_result],
        output_table="economy_state_analysis",
        if_exists="append",
        context=context,
    )

    analysis_summary = extract_economy_state_summary(result["analysis_content"])

    result_metadata = {
        "analysis_completed": True,
        "analysis_timestamp": result["analysis_timestamp"],
        "model_name": result["model_name"],
        "personality": result["personality"],
        "output_table": "economy_state_analysis",
        "records_written": 1,
        "data_sources": result["data_sources"],
        "analysis_summary": analysis_summary,
        "analysis_preview": result["analysis_content"][:500]
        if result["analysis_content"]
        else "",
    }

    context.log.info(f"Economy state analysis complete: {result_metadata}")
    return dg.MaterializeResult(metadata=result_metadata)
