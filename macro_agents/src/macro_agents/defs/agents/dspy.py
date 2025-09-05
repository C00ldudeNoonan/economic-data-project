import dagster as dg
import dspy
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from .resources.motherduck import MotherDuckResource

# Configure DSPy with OpenAI
lm = dspy.LM("openai/gpt-4o-mini", api_key=dg.EnvVar("OPENAI_API_KEY"))
dspy.configure(lm=lm)


# Data Models
class MarketData(BaseModel):
    """Market performance data structure"""

    symbol: str
    asset_type: str
    time_period: str
    total_return_pct: float
    volatility_pct: float
    win_rate_pct: float
    trading_days: int
    period_start_date: str
    period_end_date: str


class HousingData(BaseModel):
    """Housing market data structure"""

    date: str
    inventory_count: int
    median_price: float
    mortgage_rate: float
    population: int


class EconomicIndicator(BaseModel):
    """Economic indicator data structure"""

    series_id: str
    date: str
    value: float
    series_name: str


class FinancialAnalysisResult(BaseModel):
    """Result of financial analysis"""

    analysis_type: str
    key_insights: List[str]
    recommendations: List[str]
    risk_assessment: str
    market_outlook: str
    confidence_score: float


# Financial Analysis Agent using ReAct pattern
class FinancialAnalysisAgent:
    """AI agent for analyzing economic and financial data using DSPy ReAct pattern"""

    def __init__(self, motherduck_resource: MotherDuckResource):
        self.motherduck = motherduck_resource
        self.agent = self._create_agent()

    def _create_agent(self):
        """Create the DSPy ReAct agent with financial analysis tools"""

        # Define tools for financial analysis
        def analyze_market_performance(
            symbols: List[str], time_period: str = "1_year"
        ) -> Dict[str, Any]:
            """Analyze market performance for given symbols and time period"""
            try:
                data = self.motherduck.read_data("major_indicies_summary")
                filtered_data = [
                    row
                    for row in data
                    if row.get("symbol") in symbols
                    and row.get("time_period") == time_period
                ]
                return {
                    "status": "success",
                    "data": filtered_data,
                    "analysis": f"Found {len(filtered_data)} records for {symbols} in {time_period} period",
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}

        def analyze_currency_trends(
            currencies: List[str], time_period: str = "1_year"
        ) -> Dict[str, Any]:
            """Analyze currency performance trends"""
            try:
                data = self.motherduck.read_data("currency_summary")
                filtered_data = [
                    row
                    for row in data
                    if row.get("symbol") in currencies
                    and row.get("time_period") == time_period
                ]
                return {
                    "status": "success",
                    "data": filtered_data,
                    "analysis": f"Found {len(filtered_data)} currency records for {currencies} in {time_period} period",
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}

        def analyze_sector_rotation(
            sectors: List[str], time_period: str = "1_year"
        ) -> Dict[str, Any]:
            """Analyze sector performance and rotation patterns"""
            try:
                data = self.motherduck.read_data("us_sector_summary")
                filtered_data = [
                    row
                    for row in data
                    if row.get("symbol") in sectors
                    and row.get("time_period") == time_period
                ]
                return {
                    "status": "success",
                    "data": filtered_data,
                    "analysis": f"Found {len(filtered_data)} sector records for {sectors} in {time_period} period",
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}

        def analyze_housing_market(time_period: str = "1_year") -> Dict[str, Any]:
            """Analyze housing market conditions and trends"""
            try:
                data = self.motherduck.read_data("housing_inventory_latest_aggregates")
                return {
                    "status": "success",
                    "data": data,
                    "analysis": f"Retrieved {len(data)} housing market records for {time_period} period",
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}

        def analyze_economic_indicators(
            indicators: List[str], time_period: str = "1_year"
        ) -> Dict[str, Any]:
            """Analyze key economic indicators from FRED data"""
            try:
                data = self.motherduck.read_data("fred_series_latest_aggregates")
                filtered_data = [
                    row
                    for row in data
                    if any(
                        indicator in row.get("series_id", "")
                        for indicator in indicators
                    )
                ]
                return {
                    "status": "success",
                    "data": filtered_data,
                    "analysis": f"Found {len(filtered_data)} economic indicator records for {indicators}",
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}

        def compare_global_markets(time_period: str = "1_year") -> Dict[str, Any]:
            """Compare performance across global markets"""
            try:
                data = self.motherduck.read_data("global_markets_summary")
                filtered_data = [
                    row for row in data if row.get("time_period") == time_period
                ]
                return {
                    "status": "success",
                    "data": filtered_data,
                    "analysis": f"Retrieved {len(filtered_data)} global market records for {time_period} period",
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}

        def generate_financial_report(analysis_data: Dict[str, Any]) -> Dict[str, Any]:
            """Generate comprehensive financial analysis report"""
            try:
                # This would typically involve more sophisticated analysis
                # For now, we'll return a structured summary
                return {
                    "status": "success",
                    "report": {
                        "summary": "Financial analysis completed",
                        "data_sources": list(analysis_data.keys()),
                        "total_records": sum(
                            len(data.get("data", []))
                            for data in analysis_data.values()
                            if data.get("status") == "success"
                        ),
                    },
                }
            except Exception as e:
                return {"status": "error", "message": str(e)}

        # Create the ReAct agent with all tools
        tools = [
            analyze_market_performance,
            analyze_currency_trends,
            analyze_sector_rotation,
            analyze_housing_market,
            analyze_economic_indicators,
            compare_global_markets,
            generate_financial_report,
        ]

        # Define the signature for the financial analysis task
        class FinancialAnalysisSignature(dspy.Signature):
            """Analyze financial and economic data to provide insights and recommendations"""

            user_request: str = dspy.InputField(
                desc="The financial analysis request from the user"
            )
            trajectory: str = dspy.InputField(
                desc="Previous analysis steps and observations"
            )
            reasoning: str = dspy.OutputField(
                desc="Step-by-step reasoning for the analysis approach"
            )
            process_result: str = dspy.OutputField(
                desc="Summary of analysis results and key findings"
            )

        # Create the ReAct agent
        agent = dspy.ReAct(FinancialAnalysisSignature, tools=tools, max_iters=10)

        return agent

    def analyze_financial_data(self, user_request: str) -> Dict[str, Any]:
        """Main method to analyze financial data based on user request"""
        try:
            # Initialize trajectory for the agent
            trajectory = ""

            # Run the agent
            result = self.agent(user_request=user_request, trajectory=trajectory)

            return {
                "status": "success",
                "reasoning": result.reasoning,
                "process_result": result.process_result,
                "trajectory": result.trajectory,
            }

        except Exception as e:
            return {"status": "error", "message": f"Analysis failed: {str(e)}"}


# Dagster asset for financial analysis
@dg.asset(
    description="AI-powered financial analysis using DSPy ReAct agent",
    compute_kind="dspy",
)
def financial_analysis_asset(
    context: dg.AssetExecutionContext, motherduck_resource: MotherDuckResource
) -> Dict[str, Any]:
    """Analyze financial data using AI agent"""

    # Create the financial analysis agent
    agent = FinancialAnalysisAgent(motherduck_resource)

    # Example analysis request - this could be parameterized
    analysis_request = """
    Please analyze the current market conditions by:
    1. Comparing major indices performance over the past year
    2. Analyzing currency trends for major pairs
    3. Examining sector rotation patterns
    4. Assessing housing market conditions
    5. Reviewing key economic indicators
    
    Provide insights on market outlook and investment recommendations.
    """

    # Run the analysis
    result = agent.analyze_financial_data(analysis_request)

    context.log.info(f"Financial analysis completed: {result.get('status')}")

    return result


# Utility function for custom analysis requests
def analyze_market_data(
    datasets: List[str],
    motherduck_resource: MotherDuckResource,
    custom_request: Optional[str] = None,
) -> Dict[str, Any]:
    """Analyze specific market datasets with custom request"""

    agent = FinancialAnalysisAgent(motherduck_resource)

    if custom_request is None:
        custom_request = f"""
        Analyze the following datasets: {", ".join(datasets)}
        Provide comprehensive insights on market trends, risk factors, and investment opportunities.
        """

    return agent.analyze_financial_data(custom_request)
