"""Domain-specific sub-agents for economic analysis.

This module contains 5 specialized DSPy signatures and modules for analyzing
different domains of economic data:
1. Labor Market
2. Financial Conditions
3. Commodities
4. Sectors
5. Market Structure

Each sub-agent produces a focused analysis that feeds into the aggregator.
"""

import dspy


# =============================================================================
# LABOR MARKET SUB-AGENT
# =============================================================================


class LaborMarketAnalysisSignature(dspy.Signature):
    """Analyze labor market conditions and employment trends."""

    labor_data: str = dspy.InputField(
        desc="CSV data containing labor market indicators including unemployment rate, "
        "job openings, initial claims, payrolls, wages, labor force participation, "
        "and quits rate with current values and percentage changes"
    )

    employment_trends: str = dspy.InputField(
        desc="CSV data containing month-over-month changes for employment-related "
        "indicators showing momentum and direction"
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical' (bearish), 'neutral' (balanced), "
        "or 'bullish' (optimistic)"
    )

    labor_analysis: str = dspy.OutputField(
        desc="""Structured labor market analysis including:
        1. Employment Health Score (0-100): Overall assessment of labor market strength
        2. Key Metrics Summary:
           - Unemployment rate level and trend
           - Job openings and hiring pace
           - Wage growth (average hourly earnings)
           - Labor force participation
        3. Trend Direction: improving/stable/deteriorating with supporting evidence
        4. Leading Signals:
           - Initial claims trajectory (leading indicator)
           - Quits rate (confidence indicator)
           - Job openings to unemployed ratio
        5. Cycle Implications:
           - Early cycle: Rising employment, falling unemployment
           - Expansion: Tight labor market, wage pressures
           - Late cycle: Peak employment, slowing job growth
           - Recession: Rising unemployment, falling openings
        6. Risk Factors: Labor market vulnerabilities and warning signs

        Personality Guidelines:
        - SKEPTICAL: Emphasize labor market weaknesses, wage stagnation risks,
          potential layoffs, and signs of cooling
        - NEUTRAL: Balanced view of labor strengths and weaknesses
        - BULLISH: Emphasize job growth resilience, wage gains, low unemployment
        """
    )


class LaborMarketModule(dspy.Module):
    """DSPy module for labor market analysis."""

    def __init__(self, personality: str = "neutral"):
        super().__init__()
        self.personality = personality
        self.analyze = dspy.ChainOfThought(LaborMarketAnalysisSignature)

    def forward(
        self,
        labor_data: str,
        employment_trends: str = "",
        personality: str | None = None,
    ):
        return self.analyze(
            labor_data=labor_data,
            employment_trends=employment_trends or "No trend data available",
            personality=personality or self.personality,
        )


# =============================================================================
# FINANCIAL CONDITIONS SUB-AGENT
# =============================================================================


class FinancialConditionsAnalysisSignature(dspy.Signature):
    """Analyze financial conditions, monetary policy, and credit environment."""

    fci_data: str = dspy.InputField(
        desc="CSV data containing Financial Conditions Index (FCI) values with "
        "date, FCI score, and component scores. FCI > 0 = expansionary, < 0 = contractionary"
    )

    yield_curve_data: str = dspy.InputField(
        desc="CSV data containing Treasury yield curve with key spreads (10Y-2Y, 10Y-3M) "
        "and curve shape classification. Inverted curves signal recession risk"
    )

    credit_data: str = dspy.InputField(
        desc="CSV data containing credit spreads (high yield, investment grade), "
        "Fed funds rate, mortgage rates, and lending conditions"
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical' (bearish), 'neutral' (balanced), "
        "or 'bullish' (optimistic)"
    )

    financial_analysis: str = dspy.OutputField(
        desc="""Structured financial conditions analysis including:
        1. Financial Conditions Score (0-100): Overall tightness/looseness
        2. FCI Analysis:
           - Current FCI value and interpretation
           - FCI trend (improving/deteriorating)
           - Historical percentile position
           - Component breakdown (equity, credit, rates, housing, dollar)
        3. Yield Curve Assessment:
           - Current shape (steep/normal/flat/inverted)
           - Key spreads (10Y-2Y, 10Y-3M)
           - Recession probability signal
           - Historical context
        4. Credit Environment:
           - High yield spreads (risk appetite indicator)
           - Investment grade spreads
           - Lending standards (tightening/easing)
        5. Monetary Policy Stance:
           - Fed funds rate level and trajectory
           - Real rates assessment
           - Policy outlook implications
        6. Cycle Implications:
           - Early cycle: Loose conditions, steep curve
           - Expansion: Tightening conditions, flattening curve
           - Late cycle: Tight conditions, flat/inverted curve
           - Recession: Easing conditions, steepening curve
        7. Risk Factors: Credit stress signals, liquidity concerns

        Personality Guidelines:
        - SKEPTICAL: Emphasize tightening conditions, inversion risks, credit stress
        - NEUTRAL: Balanced assessment of financial environment
        - BULLISH: Emphasize accommodative conditions, healthy spreads
        """
    )


class FinancialConditionsModule(dspy.Module):
    """DSPy module for financial conditions analysis."""

    def __init__(self, personality: str = "neutral"):
        super().__init__()
        self.personality = personality
        self.analyze = dspy.ChainOfThought(FinancialConditionsAnalysisSignature)

    def forward(
        self,
        fci_data: str,
        yield_curve_data: str = "",
        credit_data: str = "",
        personality: str | None = None,
    ):
        return self.analyze(
            fci_data=fci_data,
            yield_curve_data=yield_curve_data or "No yield curve data available",
            credit_data=credit_data or "No credit data available",
            personality=personality or self.personality,
        )


# =============================================================================
# COMMODITIES SUB-AGENT
# =============================================================================


class CommoditiesAnalysisSignature(dspy.Signature):
    """Analyze commodity markets across energy, industrial, and agricultural sectors."""

    energy_data: str = dspy.InputField(
        desc="CSV data containing energy commodity performance (oil, gas, coal) "
        "with returns, volatility, and price trends"
    )

    input_commodities_data: str = dspy.InputField(
        desc="CSV data containing industrial/input commodity performance "
        "(copper, steel, aluminum, lumber) with returns and trends"
    )

    agriculture_data: str = dspy.InputField(
        desc="CSV data containing agricultural commodity performance "
        "(corn, wheat, soybeans, livestock) with returns and trends"
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical' (bearish), 'neutral' (balanced), "
        "or 'bullish' (optimistic)"
    )

    commodity_analysis: str = dspy.OutputField(
        desc="""Structured commodity market analysis including:
        1. Commodity Cycle Score (0-100): Overall commodity market health
        2. Energy Sector Analysis:
           - Oil price trends and implications
           - Natural gas dynamics
           - Energy as economic activity indicator
           - Inflation pressure from energy
        3. Industrial Commodities Analysis:
           - Copper (Dr. Copper as economic indicator)
           - Steel and aluminum (manufacturing signals)
           - Lumber (housing/construction proxy)
           - Supply chain stress signals
        4. Agricultural Commodities Analysis:
           - Food commodity inflation pressures
           - Key crop trends (corn, wheat, soybeans)
           - Consumer impact assessment
        5. Inflation Signals:
           - Commodity-driven inflation pressures
           - Input cost trends for businesses
           - Consumer price implications
        6. Cycle Implications:
           - Early cycle: Rising industrial commodities
           - Expansion: Broad commodity strength
           - Late cycle: Peak commodity prices
           - Recession: Falling commodity demand
        7. Risk Factors: Supply shocks, geopolitical risks, weather impacts

        Personality Guidelines:
        - SKEPTICAL: Emphasize commodity weakness, demand concerns, deflation risks
        - NEUTRAL: Balanced view of commodity dynamics
        - BULLISH: Emphasize commodity strength, demand resilience, inflation hedging
        """
    )


class CommoditiesModule(dspy.Module):
    """DSPy module for commodities analysis."""

    def __init__(self, personality: str = "neutral"):
        super().__init__()
        self.personality = personality
        self.analyze = dspy.ChainOfThought(CommoditiesAnalysisSignature)

    def forward(
        self,
        energy_data: str,
        input_commodities_data: str = "",
        agriculture_data: str = "",
        personality: str | None = None,
    ):
        return self.analyze(
            energy_data=energy_data,
            input_commodities_data=input_commodities_data
            or "No industrial commodities data available",
            agriculture_data=agriculture_data
            or "No agricultural commodities data available",
            personality=personality or self.personality,
        )


# =============================================================================
# SECTOR SUB-AGENT
# =============================================================================


class SectorAnalysisSignature(dspy.Signature):
    """Analyze US equity sector rotation and relative performance."""

    sector_data: str = dspy.InputField(
        desc="CSV data containing US sector ETF performance (XLK, XLF, XLE, XLV, etc.) "
        "with returns, volatility, and relative strength metrics"
    )

    correlation_data: str = dspy.InputField(
        desc="CSV data containing correlations between sectors and economic indicators, "
        "showing which sectors lead/lag the economic cycle"
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical' (bearish), 'neutral' (balanced), "
        "or 'bullish' (optimistic)"
    )

    sector_analysis: str = dspy.OutputField(
        desc="""Structured sector rotation analysis including:
        1. Sector Rotation Score (0-100): Cycle positioning confidence
        2. Sector Performance Rankings:
           - Top performing sectors with returns
           - Bottom performing sectors with returns
           - Relative strength trends
        3. Cyclical vs Defensive Balance:
           - Cyclical sectors: Technology, Financials, Industrials, Consumer Discretionary
           - Defensive sectors: Utilities, Consumer Staples, Healthcare
           - Current market preference signal
        4. Sector Rotation Signals:
           - Early cycle leaders: Financials, Consumer Discretionary
           - Mid cycle leaders: Technology, Industrials
           - Late cycle leaders: Energy, Materials
           - Recession leaders: Utilities, Consumer Staples, Healthcare
        5. Key Sector Insights:
           - Technology (XLK): Growth/innovation signal
           - Financials (XLF): Credit/lending environment
           - Energy (XLE): Commodity cycle proxy
           - Industrials (XLI): Manufacturing/capex signal
        6. Cycle Implications:
           - Which phase does current rotation suggest?
           - Confidence level in rotation signal
           - Historical pattern comparison
        7. Risk Factors: Concentration risks, sector-specific headwinds

        Personality Guidelines:
        - SKEPTICAL: Emphasize defensive sector strength, cyclical weakness, rotation to safety
        - NEUTRAL: Balanced sector assessment without directional bias
        - BULLISH: Emphasize cyclical sector strength, growth leadership, risk-on rotation
        """
    )


class SectorModule(dspy.Module):
    """DSPy module for sector analysis."""

    def __init__(self, personality: str = "neutral"):
        super().__init__()
        self.personality = personality
        self.analyze = dspy.ChainOfThought(SectorAnalysisSignature)

    def forward(
        self,
        sector_data: str,
        correlation_data: str = "",
        personality: str | None = None,
    ):
        return self.analyze(
            sector_data=sector_data,
            correlation_data=correlation_data or "No correlation data available",
            personality=personality or self.personality,
        )


# =============================================================================
# MARKET STRUCTURE SUB-AGENT
# =============================================================================


class MarketStructureAnalysisSignature(dspy.Signature):
    """Analyze broad market structure including indices, fixed income, and global markets."""

    indices_data: str = dspy.InputField(
        desc="CSV data containing major index performance (SPY, QQQ, DIA, IWM, VIX) "
        "with returns, volatility, and breadth metrics"
    )

    fixed_income_data: str = dspy.InputField(
        desc="CSV data containing fixed income ETF performance (HYG, LQD, TIP, GOVT) "
        "representing credit and duration positioning"
    )

    global_markets_data: str = dspy.InputField(
        desc="CSV data containing global market ETFs (EEM, EFA, FXI) and currency "
        "indicators showing international capital flows"
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical' (bearish), 'neutral' (balanced), "
        "or 'bullish' (optimistic)"
    )

    market_analysis: str = dspy.OutputField(
        desc="""Structured market structure analysis including:
        1. Market Health Score (0-100): Overall market condition assessment
        2. Equity Market Assessment:
           - Major indices performance (S&P 500, Nasdaq, Dow, Russell 2000)
           - Large cap vs small cap divergence
           - Growth vs value dynamics
           - Market breadth signals
        3. Volatility Analysis:
           - VIX level and trend
           - Volatility regime (low/normal/elevated/extreme)
           - Risk sentiment indicator
        4. Fixed Income Signals:
           - Credit vs government bond preference
           - High yield performance (risk appetite)
           - Duration positioning
           - TIPS performance (inflation expectations)
        5. Global Market Context:
           - US vs international relative performance
           - Emerging markets sentiment
           - Dollar strength impact
           - Capital flow direction
        6. Risk Appetite Assessment:
           - Risk-on vs risk-off positioning
           - Cross-asset confirmation
           - Divergences and warning signs
        7. Cycle Implications:
           - Early cycle: Rising equities, credit outperformance
           - Expansion: Broad market strength, low volatility
           - Late cycle: Narrowing leadership, rising volatility
           - Recession: Defensive positioning, credit weakness
        8. Risk Factors: Market concentration, liquidity concerns, global risks

        Personality Guidelines:
        - SKEPTICAL: Emphasize market risks, volatility concerns, defensive signals
        - NEUTRAL: Balanced market assessment
        - BULLISH: Emphasize market strength, breadth improvement, risk-on signals
        """
    )


class MarketStructureModule(dspy.Module):
    """DSPy module for market structure analysis."""

    def __init__(self, personality: str = "neutral"):
        super().__init__()
        self.personality = personality
        self.analyze = dspy.ChainOfThought(MarketStructureAnalysisSignature)

    def forward(
        self,
        indices_data: str,
        fixed_income_data: str = "",
        global_markets_data: str = "",
        personality: str | None = None,
    ):
        return self.analyze(
            indices_data=indices_data,
            fixed_income_data=fixed_income_data or "No fixed income data available",
            global_markets_data=global_markets_data
            or "No global markets data available",
            personality=personality or self.personality,
        )
