import dspy


class EconomyStateAnalysisSignature(dspy.Signature):
    """Analyze current economic indicators to determine the state of the economy."""

    economic_data: str = dspy.InputField(
        desc="CSV data containing latest economic indicators with current values and percentage changes over 3m, 6m, 1y periods"
    )

    commodity_data: str = dspy.InputField(
        desc="CSV data containing commodity price performance across energy, industrial/input, and agricultural commodities with returns, volatility, and trends over different time periods"
    )

    financial_conditions_index: str = dspy.InputField(
        desc="CSV data containing full historical Financial Conditions Index (FCI) values with date, FCI score, and component scores. FCI values above zero indicate expansionary conditions, below zero indicate contractionary conditions. Use the full history to identify trends, cycles, and current position relative to historical norms."
    )

    housing_data: str = dspy.InputField(
        desc="CSV data containing housing market indicators including inventory levels (with 3m, 6m, 1y changes) and mortgage rates with affordability metrics (median prices, monthly payments). Housing is a leading economic indicator - rising inventory suggests slowing demand, while affordability metrics indicate consumer purchasing power."
    )

    yield_curve_data: str = dspy.InputField(
        desc="CSV data containing Treasury yield curve data with key spreads (10Y-2Y, 10Y-3M) and curve shape classification (Steep/Normal/Flat/Inverted). Inverted yield curves (10Y-2Y < 0) historically precede recessions. Normal curves indicate healthy economic expectations."
    )

    economic_trends: str = dspy.InputField(
        desc="CSV data containing month-over-month changes for key economic indicators (GDP, CPI, unemployment, payrolls, Fed funds rate, industrial production, retail sales, housing starts). These trends show momentum and direction of economic activity."
    )

    personality: str = dspy.InputField(
        desc="Analytical personality: 'skeptical' (default, bearish, focuses on risks and downside), 'neutral' (balanced, objective), or 'bullish' (optimistic, focuses on opportunities and upside)"
    )

    analysis: str = dspy.OutputField(
        desc="""Comprehensive economic state analysis reflecting the specified personality perspective, including:
        1. Current Economic Cycle Position (Early/Expansion/Late/Recession with confidence level 0-1)
        2. Financial Conditions Index Analysis:
           - Current FCI value and its interpretation (expansionary if >0, contractionary if <0)
           - FCI trend over recent periods (3m, 6m, 1y) to assess whether conditions are improving or deteriorating
           - Historical context: current FCI position relative to historical range, percentiles, and past cycles
           - Component analysis: which financial indicators (equity, credit spreads, rates, housing, dollar) are driving FCI changes
           - FCI as a leading indicator: how current FCI levels and trends relate to economic expansion/contraction phases
           - Comparison of FCI trajectory with other economic indicators for consistency
        3. Key Economic Indicators Analysis:
           - GDP growth trends and outlook
           - Inflation levels and trajectory (CPI, PCE, core inflation)
           - Employment metrics (unemployment rate, job growth, labor force participation)
           - Interest rates and monetary policy stance
           - Consumer sentiment and spending indicators
           - Housing market indicators
           - Manufacturing and services PMI
        4. Commodity Market Analysis:
           - Energy commodity trends (oil, gas, coal) and implications for economic activity
           - Industrial/input commodity prices (metals, materials) and manufacturing signals
           - Agricultural commodity prices and food inflation pressures
           - Commodity price trends as leading indicators of economic activity
           - Supply chain and cost pressure signals from commodity markets
        5. Leading Indicators Assessment:
           - Yield curve analysis (normal, flat, inverted) - use yield_curve_data to assess curve shape and spreads (10Y-2Y, 10Y-3M). Inverted curves signal recession risk.
           - Credit spreads and financial conditions (integrate with FCI analysis)
           - Business confidence indicators
           - Leading economic index components
           - Commodity price momentum as economic signals
           - Housing market trends from housing_data - inventory levels, affordability, mortgage rates
           - Economic momentum from economic_trends - month-over-month changes in key indicators
        6. Economic Cycle Phase Characteristics:
           - Current phase identification with supporting evidence (use FCI to confirm expansion/contraction)
           - Phase duration and typical progression patterns
           - Key indicators suggesting phase transitions (FCI trends are particularly important here)
           - Commodity cycle alignment with economic cycle
           - FCI cycle alignment with economic cycle
        7. Risk Factors (skeptical: emphasize risks, neutral: balanced, bullish: acknowledge but minimize):
           - Economic imbalances or vulnerabilities
           - Potential inflection points (watch for FCI turning points)
           - External risks (geopolitical, trade, etc.)
           - Commodity price shocks and their economic impact
           - Financial conditions tightening or loosening (from FCI analysis)
        
        Personality Guidelines:
        - SKEPTICAL/BEARISH: Emphasize downside risks, vulnerabilities, potential recessions, negative indicators. Be cautious and highlight what could go wrong. Focus on defensive positioning. If FCI is declining or negative, emphasize contractionary risks.
        - NEUTRAL: Provide balanced, objective analysis weighing both positive and negative factors equally. Avoid extreme positions. Present FCI data objectively.
        - BULLISH/HOPEFUL: Emphasize opportunities, positive trends, resilience, and upside potential. Highlight strengths and growth prospects. Focus on expansionary positioning. If FCI is positive or improving, emphasize expansionary conditions.
        
        Focus on quantitative metrics, trends, and leading indicators including commodity markets and the Financial Conditions Index to provide a clear assessment of the current economic state from the specified perspective."""
    )
