"""Economic State Aggregator Module.

This module contains the DSPy signature and module for aggregating outputs
from the 5 domain sub-agents into a comprehensive economic state analysis.
"""

import dspy


class EconomicStateAggregatorSignature(dspy.Signature):
    """Synthesize domain analyses into comprehensive economic state assessment."""

    labor_analysis: str = dspy.InputField(
        desc="Labor market sub-agent output containing employment health score, "
        "key metrics, trends, and cycle implications"
    )

    financial_analysis: str = dspy.InputField(
        desc="Financial conditions sub-agent output containing FCI analysis, "
        "yield curve assessment, credit environment, and monetary policy stance"
    )

    commodity_analysis: str = dspy.InputField(
        desc="Commodities sub-agent output containing energy, industrial, and "
        "agricultural commodity analysis with inflation signals"
    )

    sector_analysis: str = dspy.InputField(
        desc="Sector rotation sub-agent output containing sector rankings, "
        "cyclical vs defensive balance, and rotation signals"
    )

    market_analysis: str = dspy.InputField(
        desc="Market structure sub-agent output containing indices assessment, "
        "fixed income signals, global context, and risk appetite"
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical' (bearish), 'neutral' (balanced), "
        "or 'bullish' (optimistic)"
    )

    economic_state: str = dspy.OutputField(
        desc="""Comprehensive economic state synthesis including:

        1. ECONOMIC CYCLE POSITION
           - Current Phase: Early / Expansion / Late / Recession
           - Confidence Level: 0.0 to 1.0
           - Phase Duration Estimate: How long in current phase
           - Transition Probability: Likelihood of moving to next phase

        2. CROSS-DOMAIN SIGNAL SUMMARY
           - Labor Market Signal: [Bullish/Neutral/Bearish] - key insight
           - Financial Conditions Signal: [Bullish/Neutral/Bearish] - key insight
           - Commodity Signal: [Bullish/Neutral/Bearish] - key insight
           - Sector Rotation Signal: [Bullish/Neutral/Bearish] - key insight
           - Market Structure Signal: [Bullish/Neutral/Bearish] - key insight

        3. CROSS-DOMAIN CORRELATIONS
           - Agreements: Where multiple domains confirm the same signal
           - Conflicts: Where domains show divergent signals (important!)
           - Resolution: How to interpret conflicting signals

        4. LEADING INDICATOR SUMMARY
           - Top 5 leading indicators and their current readings
           - What they suggest about the next 3-6 months
           - Confidence in forward outlook

        5. ECONOMIC HEALTH METRICS
           - Overall Economic Health Score: 0-100
           - Growth Momentum: Accelerating / Stable / Decelerating
           - Inflation Pressure: High / Moderate / Low
           - Financial Stress: High / Moderate / Low

        6. RISK ASSESSMENT
           - Primary Risks: Top 3 risks to monitor
           - Tail Risks: Low probability, high impact scenarios
           - Risk Level: Overall risk environment assessment

        7. KEY ACTIONABLE INSIGHTS
           - 3-5 most important takeaways from the analysis
           - What changed since last analysis (if applicable)
           - What to watch in the coming period

        8. DATA QUALITY NOTES
           - Any data gaps or limitations
           - Confidence adjustments based on data quality
           - Areas needing more investigation

        Personality Guidelines:
        - SKEPTICAL/BEARISH: Weight negative signals more heavily, emphasize risks,
          lower confidence in positive signals, favor defensive interpretation
        - NEUTRAL: Equal weight to all signals, balanced risk assessment,
          acknowledge uncertainty without directional bias
        - BULLISH/OPTIMISTIC: Weight positive signals more heavily, emphasize
          opportunities, higher confidence in growth signals, favor constructive interpretation
        """
    )


class EconomicStateAggregatorModule(dspy.Module):
    """DSPy module for aggregating domain analyses into economic state."""

    def __init__(self, personality: str = "neutral"):
        super().__init__()
        self.personality = personality
        self.aggregate = dspy.ChainOfThought(EconomicStateAggregatorSignature)

    def forward(
        self,
        labor_analysis: str,
        financial_analysis: str,
        commodity_analysis: str,
        sector_analysis: str,
        market_analysis: str,
        personality: str | None = None,
    ):
        return self.aggregate(
            labor_analysis=labor_analysis,
            financial_analysis=financial_analysis,
            commodity_analysis=commodity_analysis,
            sector_analysis=sector_analysis,
            market_analysis=market_analysis,
            personality=personality or self.personality,
        )


class DomainAnalysisResult:
    """Container for storing results from all domain sub-agents."""

    def __init__(
        self,
        labor_analysis: str = "",
        financial_analysis: str = "",
        commodity_analysis: str = "",
        sector_analysis: str = "",
        market_analysis: str = "",
    ):
        self.labor_analysis = labor_analysis
        self.financial_analysis = financial_analysis
        self.commodity_analysis = commodity_analysis
        self.sector_analysis = sector_analysis
        self.market_analysis = market_analysis

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "labor_analysis": self.labor_analysis,
            "financial_analysis": self.financial_analysis,
            "commodity_analysis": self.commodity_analysis,
            "sector_analysis": self.sector_analysis,
            "market_analysis": self.market_analysis,
        }

    def all_complete(self) -> bool:
        """Check if all domain analyses are complete."""
        return all(
            [
                self.labor_analysis,
                self.financial_analysis,
                self.commodity_analysis,
                self.sector_analysis,
                self.market_analysis,
            ]
        )

    def summary(self) -> str:
        """Get a brief summary of completion status."""
        domains = {
            "labor": bool(self.labor_analysis),
            "financial": bool(self.financial_analysis),
            "commodity": bool(self.commodity_analysis),
            "sector": bool(self.sector_analysis),
            "market": bool(self.market_analysis),
        }
        complete = sum(domains.values())
        return f"{complete}/5 domains complete: {domains}"
