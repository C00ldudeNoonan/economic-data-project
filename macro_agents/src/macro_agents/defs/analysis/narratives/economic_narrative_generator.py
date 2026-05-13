"""Economic Narrative Generator using DSPy.

This module generates plain-English narratives for economic indicator releases,
translating data into actionable insights for retail investors.

The primary differentiator: "We translate the same data that moves billion-dollar
decisions into insights anyone can act on."
"""

import dspy


# =============================================================================
# INDICATOR NARRATIVE SIGNATURES
# =============================================================================


class EconomicIndicatorNarrativeSignature(dspy.Signature):
    """Generate a plain-English narrative for an economic indicator release."""

    indicator_name: str = dspy.InputField(
        desc="Name of the economic indicator (e.g., 'CPI', 'Nonfarm Payrolls', 'GDP')"
    )

    indicator_category: str = dspy.InputField(
        desc="Category of indicator: 'inflation', 'employment', 'growth', 'housing', "
        "'consumer', 'manufacturing', 'trade', 'monetary_policy'"
    )

    current_value: str = dspy.InputField(
        desc="Current value of the indicator with units (e.g., '3.2%', '256,000 jobs', '$28.50/hour')"
    )

    previous_value: str = dspy.InputField(desc="Previous period value for comparison")

    expected_value: str = dspy.InputField(
        desc="Consensus economist expectation/forecast"
    )

    historical_context: str = dspy.InputField(
        desc="CSV data containing historical values for this indicator over past periods "
        "(monthly/quarterly) to identify trends and provide context"
    )

    related_indicators: str = dspy.InputField(
        desc="CSV data containing related economic indicators that provide additional context "
        "(e.g., for CPI: Core CPI, PCE, inflation expectations)"
    )

    current_fed_policy: str = dspy.InputField(
        desc="Current Fed policy stance and recent FOMC statements relevant to this indicator"
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical' (cautious), 'neutral' (balanced), "
        "or 'bullish' (optimistic)"
    )

    headline: str = dspy.OutputField(
        desc="A concise, attention-grabbing headline (max 100 characters) that captures "
        "the key takeaway. Examples: 'Inflation Cools More Than Expected, Fed Pivot Hopes Rise', "
        "'Jobs Report Crushes Expectations, Soft Landing Back in Play'"
    )

    summary: str = dspy.OutputField(
        desc="""A 2-3 paragraph plain-English summary including:
        1. What happened: The actual number vs expectations and previous value
        2. Why it matters: What this tells us about the economy
        3. The trend: Whether conditions are improving, stable, or deteriorating

        Write for an intelligent reader who isn't an economist. Avoid jargon.
        Use concrete comparisons (e.g., 'fastest pace since 2021' not 'elevated')."""
    )

    market_implications: str = dspy.OutputField(
        desc="""Market implications analysis including:
        1. Stock market impact: How this typically affects equity markets
        2. Bond market impact: What this means for interest rates and bonds
        3. Sector implications: Which sectors benefit or suffer
        4. Fed policy implications: How this affects rate expectations

        Be specific about direction and magnitude expectations.
        Personality Guidelines:
        - SKEPTICAL: Emphasize risks, potential negative market reactions
        - NEUTRAL: Balanced view of upside and downside
        - BULLISH: Emphasize positive implications and opportunities"""
    )

    retail_investor_takeaway: str = dspy.OutputField(
        desc="""'What This Means For You' section written for retail investors:
        1. Portfolio positioning: Should you adjust allocations?
        2. Timing considerations: Is this a buy/hold/reduce signal?
        3. Sectors to watch: Specific ETFs or sectors affected
        4. Risk awareness: What to monitor going forward

        Write in first person ('you should consider...') and be actionable.
        Avoid generic advice like 'stay diversified' - be specific."""
    )

    historical_comparison: str = dspy.OutputField(
        desc="""Historical context phrase that anchors the data point:
        Examples:
        - 'Highest reading since March 2008'
        - 'Third consecutive monthly decline'
        - 'Largest miss vs expectations in 2 years'
        - 'Returns to pre-pandemic levels'
        - 'Marks 18 months above Fed target'

        Provide 2-3 specific historical comparisons that give perspective."""
    )

    confidence_level: str = dspy.OutputField(
        desc="Confidence level (high/medium/low) in the analysis with brief explanation"
    )


class IndicatorForecastSignature(dspy.Signature):
    """Generate a short-term forecast for an economic indicator."""

    indicator_name: str = dspy.InputField(desc="Name of the economic indicator")

    historical_data: str = dspy.InputField(
        desc="CSV data containing historical values with dates for trend analysis"
    )

    leading_indicators: str = dspy.InputField(
        desc="CSV data containing leading indicators that historically predict this indicator"
    )

    current_economic_conditions: str = dspy.InputField(
        desc="Summary of current economic conditions from economy state analysis"
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical', 'neutral', or 'bullish'"
    )

    next_release_forecast: str = dspy.OutputField(
        desc="""Forecast for the next release including:
        1. Point estimate with range (e.g., '3.1% with range 2.9-3.3%')
        2. Direction vs current (higher/lower/stable)
        3. Key factors driving the forecast
        4. Risk factors that could cause deviation"""
    )

    three_month_outlook: str = dspy.OutputField(
        desc="""Three-month trend outlook:
        1. Expected direction over next 3 months
        2. Confidence level (high/medium/low)
        3. Key catalysts to watch
        4. Scenario analysis (base case, upside, downside)"""
    )

    forecast_rationale: str = dspy.OutputField(
        desc="Clear explanation of the reasoning behind the forecast, citing specific "
        "data points and historical patterns"
    )


# =============================================================================
# NARRATIVE GENERATION MODULES
# =============================================================================


class EconomicNarrativeModule(dspy.Module):
    """DSPy module for generating economic indicator narratives."""

    def __init__(self, personality: str = "neutral"):
        super().__init__()
        self.personality = personality
        self.generate_narrative = dspy.ChainOfThought(
            EconomicIndicatorNarrativeSignature
        )

    def forward(
        self,
        indicator_name: str,
        indicator_category: str,
        current_value: str,
        previous_value: str,
        expected_value: str,
        historical_context: str,
        related_indicators: str = "",
        current_fed_policy: str = "",
        personality: str | None = None,
    ):
        return self.generate_narrative(
            indicator_name=indicator_name,
            indicator_category=indicator_category,
            current_value=current_value,
            previous_value=previous_value,
            expected_value=expected_value,
            historical_context=historical_context,
            related_indicators=related_indicators or "No related indicators available",
            current_fed_policy=current_fed_policy or "No Fed policy context available",
            personality=personality or self.personality,
        )


class IndicatorForecastModule(dspy.Module):
    """DSPy module for forecasting economic indicators."""

    def __init__(self, personality: str = "neutral"):
        super().__init__()
        self.personality = personality
        self.generate_forecast = dspy.ChainOfThought(IndicatorForecastSignature)

    def forward(
        self,
        indicator_name: str,
        historical_data: str,
        leading_indicators: str = "",
        current_economic_conditions: str = "",
        personality: str | None = None,
    ):
        return self.generate_forecast(
            indicator_name=indicator_name,
            historical_data=historical_data,
            leading_indicators=leading_indicators or "No leading indicators available",
            current_economic_conditions=current_economic_conditions
            or "No economic conditions summary available",
            personality=personality or self.personality,
        )


# =============================================================================
# INDICATOR CATEGORY CONFIGURATIONS
# =============================================================================

# Maps indicator series codes to their categories and display names
INDICATOR_CONFIGS = {
    # Inflation indicators
    "CPIAUCSL": {
        "name": "Consumer Price Index (CPI)",
        "category": "inflation",
        "frequency": "monthly",
        "related_series": ["CPILFESL", "PCEPI", "PCEPILFE", "T5YIE", "T10YIE"],
    },
    "CPILFESL": {
        "name": "Core CPI (Ex Food & Energy)",
        "category": "inflation",
        "frequency": "monthly",
        "related_series": ["CPIAUCSL", "PCEPI", "PCEPILFE"],
    },
    "PCEPI": {
        "name": "PCE Price Index",
        "category": "inflation",
        "frequency": "monthly",
        "related_series": ["PCEPILFE", "CPIAUCSL", "CPILFESL"],
    },
    "PCEPILFE": {
        "name": "Core PCE (Fed's Preferred Measure)",
        "category": "inflation",
        "frequency": "monthly",
        "related_series": ["PCEPI", "CPIAUCSL", "CPILFESL"],
    },
    # Employment indicators
    "PAYEMS": {
        "name": "Nonfarm Payrolls",
        "category": "employment",
        "frequency": "monthly",
        "related_series": ["UNRATE", "AHETPI", "ICSA", "JTSJOL"],
    },
    "UNRATE": {
        "name": "Unemployment Rate",
        "category": "employment",
        "frequency": "monthly",
        "related_series": ["PAYEMS", "U6RATE", "CIVPART", "EMRATIO"],
    },
    "ICSA": {
        "name": "Initial Jobless Claims",
        "category": "employment",
        "frequency": "weekly",
        "related_series": ["PAYEMS", "UNRATE", "JTSJOL"],
    },
    "AHETPI": {
        "name": "Average Hourly Earnings",
        "category": "employment",
        "frequency": "monthly",
        "related_series": ["PAYEMS", "CPIAUCSL", "PCEPI"],
    },
    # Growth indicators
    "GDPC1": {
        "name": "Real GDP",
        "category": "growth",
        "frequency": "quarterly",
        "related_series": ["INDPRO", "PCE", "RSXFS"],
    },
    "INDPRO": {
        "name": "Industrial Production",
        "category": "manufacturing",
        "frequency": "monthly",
        "related_series": ["TCU", "GDPC1"],
    },
    # Consumer indicators
    "RSXFS": {
        "name": "Retail Sales",
        "category": "consumer",
        "frequency": "monthly",
        "related_series": ["PCE", "UMCSENT", "PSAVERT"],
    },
    "UMCSENT": {
        "name": "Consumer Sentiment (UMich)",
        "category": "consumer",
        "frequency": "monthly",
        "related_series": ["RSXFS", "PCE", "PSAVERT"],
    },
    # Housing indicators
    "HOUST": {
        "name": "Housing Starts",
        "category": "housing",
        "frequency": "monthly",
        "related_series": ["PERMIT", "CSUSHPISA", "MORTGAGE30US"],
    },
    "CSUSHPISA": {
        "name": "Case-Shiller Home Price Index",
        "category": "housing",
        "frequency": "monthly",
        "related_series": ["HOUST", "PERMIT", "MORTGAGE30US"],
    },
    # Monetary policy indicators
    "FEDFUNDS": {
        "name": "Federal Funds Rate",
        "category": "monetary_policy",
        "frequency": "daily",
        "related_series": ["DFF", "T10Y2Y", "T10Y3M"],
    },
}


def get_indicator_config(series_code: str) -> dict:
    """Get configuration for an indicator by series code."""
    return INDICATOR_CONFIGS.get(
        series_code,
        {
            "name": series_code,
            "category": "other",
            "frequency": "unknown",
            "related_series": [],
        },
    )
