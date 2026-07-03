{{
  config(
    description='Classifies economic regimes (Expansion, Slowdown, Contraction, Recovery) using key macroeconomic indicators'
  )
}}

-- Economic Regime Classification
-- Uses: GDP growth, unemployment trend, inflation, yield curve, leading indicators
-- Regimes: Expansion, Slowdown, Contraction, Recovery

WITH monthly_indicators AS (
    -- Pivot key indicators into columns for each month
    SELECT
        DATE_TRUNC(date, MONTH) AS month_date,
        MAX(CASE WHEN series_code = 'INDPRO' THEN value END) AS industrial_production,
        MAX(CASE WHEN series_code = 'UNRATE' THEN value END) AS unemployment_rate,
        MAX(CASE WHEN series_code = 'PAYEMS' THEN value END) AS nonfarm_payrolls,
        MAX(CASE WHEN series_code = 'CPIAUCSL' THEN value END) AS cpi,
        MAX(CASE WHEN series_code = 'PCEPILFE' THEN value END) AS core_pce,
        MAX(CASE WHEN series_code = 'T10Y2Y' THEN value END) AS yield_curve_10y2y,
        MAX(CASE WHEN series_code = 'T10Y3M' THEN value END) AS yield_curve_10y3m,
        MAX(CASE WHEN series_code = 'CFNAIMA3' THEN value END) AS cfnai,
        MAX(CASE WHEN series_code = 'USSLIND' THEN value END) AS leading_index,
        MAX(CASE WHEN series_code = 'ICSA' THEN value END) AS initial_claims,
        MAX(CASE WHEN series_code = 'UMCSENT' THEN value END) AS consumer_sentiment,
        MAX(CASE WHEN series_code = 'IPMAN' THEN value END) AS mfg_production,
        MAX(CASE WHEN series_code = 'NFCI' THEN value END) AS financial_conditions
    FROM {{ ref('fred_monthly_diff') }}
    WHERE series_code IN (
        'INDPRO', 'UNRATE', 'PAYEMS', 'CPIAUCSL', 'PCEPILFE',
        'T10Y2Y', 'T10Y3M', 'CFNAIMA3', 'USSLIND', 'ICSA',
        'UMCSENT', 'IPMAN', 'NFCI'
    )
    GROUP BY DATE_TRUNC(date, MONTH)
),

indicator_changes AS (
    SELECT
        month_date,
        industrial_production,
        unemployment_rate,
        nonfarm_payrolls,
        cpi,
        core_pce,
        yield_curve_10y2y,
        yield_curve_10y3m,
        cfnai,
        leading_index,
        initial_claims,
        consumer_sentiment,
        mfg_production,
        financial_conditions,

        -- Calculate month-over-month changes
        industrial_production - LAG(industrial_production, 1) OVER (ORDER BY month_date) AS indpro_mom,
        unemployment_rate - LAG(unemployment_rate, 1) OVER (ORDER BY month_date) AS unrate_mom,
        nonfarm_payrolls - LAG(nonfarm_payrolls, 1) OVER (ORDER BY month_date) AS payrolls_mom,

        -- 3-month changes for trend assessment
        industrial_production - LAG(industrial_production, 3) OVER (ORDER BY month_date) AS indpro_3mo,
        unemployment_rate - LAG(unemployment_rate, 3) OVER (ORDER BY month_date) AS unrate_3mo,
        nonfarm_payrolls - LAG(nonfarm_payrolls, 3) OVER (ORDER BY month_date) AS payrolls_3mo,

        -- 6-month changes
        industrial_production - LAG(industrial_production, 6) OVER (ORDER BY month_date) AS indpro_6mo,
        unemployment_rate - LAG(unemployment_rate, 6) OVER (ORDER BY month_date) AS unrate_6mo,

        -- Year-over-year inflation rate
        CASE
            WHEN LAG(cpi, 12) OVER (ORDER BY month_date) > 0
            THEN ((cpi - LAG(cpi, 12) OVER (ORDER BY month_date)) / LAG(cpi, 12) OVER (ORDER BY month_date)) * 100
        END AS cpi_yoy,

        -- Leading index momentum
        leading_index - LAG(leading_index, 3) OVER (ORDER BY month_date) AS leading_3mo,
        leading_index - LAG(leading_index, 6) OVER (ORDER BY month_date) AS leading_6mo,

        -- Manufacturing production trend (YoY % change)
        CASE
            WHEN LAG(mfg_production, 12) OVER (ORDER BY month_date) > 0
            THEN ((mfg_production - LAG(mfg_production, 12) OVER (ORDER BY month_date))
                  / LAG(mfg_production, 12) OVER (ORDER BY month_date)) * 100
        END AS mfg_production_yoy

    FROM monthly_indicators
),

regime_signals AS (
    SELECT
        *,

        -- Growth Signal: Based on industrial production and leading indicators
        CASE
            WHEN indpro_3mo > 0 AND indpro_6mo > 0 AND leading_3mo > 0 THEN 2  -- Strong growth
            WHEN indpro_3mo > 0 OR leading_3mo > 0 THEN 1  -- Moderate growth
            WHEN indpro_3mo < 0 AND indpro_6mo < 0 THEN -2  -- Contraction
            WHEN indpro_3mo < 0 OR leading_6mo < 0 THEN -1  -- Slowing
            ELSE 0
        END AS growth_signal,

        -- Employment Signal: Based on unemployment and payrolls
        CASE
            WHEN unrate_3mo < -0.2 AND payrolls_3mo > 200 THEN 2  -- Strong employment
            WHEN unrate_3mo < 0 OR payrolls_3mo > 100 THEN 1  -- Improving employment
            WHEN unrate_3mo > 0.5 AND payrolls_3mo < -100 THEN -2  -- Deteriorating
            WHEN unrate_3mo > 0.2 OR payrolls_3mo < 0 THEN -1  -- Weakening
            ELSE 0
        END AS employment_signal,

        -- Inflation Signal: Based on CPI YoY
        CASE
            WHEN cpi_yoy > 4 THEN 2  -- High inflation
            WHEN cpi_yoy > 2.5 THEN 1  -- Above target
            WHEN cpi_yoy < 1 THEN -1  -- Low inflation / deflation risk
            ELSE 0  -- Near target
        END AS inflation_signal,

        -- Yield Curve Signal: Inversion indicates recession risk
        CASE
            WHEN yield_curve_10y2y < -0.5 THEN -2  -- Deeply inverted
            WHEN yield_curve_10y2y < 0 THEN -1  -- Inverted
            WHEN yield_curve_10y2y > 1.5 THEN 1  -- Steep (early recovery)
            ELSE 0  -- Normal
        END AS yield_curve_signal,

        -- Financial Conditions Signal: NFCI (negative = loose, positive = tight)
        CASE
            WHEN financial_conditions > 0.5 THEN -2  -- Tight conditions
            WHEN financial_conditions > 0 THEN -1  -- Tightening
            WHEN financial_conditions < -0.5 THEN 1  -- Loose conditions
            ELSE 0  -- Neutral
        END AS financial_signal,

        -- Manufacturing Production Signal (based on YoY % change)
        CASE
            WHEN mfg_production_yoy > 3 THEN 2   -- Strong growth
            WHEN mfg_production_yoy > 0 THEN 1   -- Moderate growth
            WHEN mfg_production_yoy > -3 THEN -1 -- Mild contraction
            ELSE -2                               -- Significant contraction
        END AS mfg_signal

    FROM indicator_changes
    WHERE month_date >= '2000-01-01'  -- Start from 2000 for consistent data
),

regime_classification AS (
    SELECT
        *,

        -- Composite score: weighted sum of signals
        (growth_signal * 2.0 + employment_signal * 1.5 + inflation_signal * 0.5
         + yield_curve_signal * 1.0 + financial_signal * 0.5 + COALESCE(mfg_signal, 0) * 1.0) AS composite_score,

        -- Regime Classification Logic
        CASE
            -- EXPANSION: Strong growth, improving employment, manageable inflation
            WHEN growth_signal >= 1 AND employment_signal >= 1 AND yield_curve_signal >= 0
            THEN 'Expansion'

            -- SLOWDOWN: Growth decelerating, employment still okay, potential yield curve warning
            WHEN (growth_signal <= 0 OR yield_curve_signal < 0)
                AND employment_signal >= 0
                AND growth_signal > -2
            THEN 'Slowdown'

            -- CONTRACTION: Negative growth, deteriorating employment
            WHEN growth_signal <= -1 AND (employment_signal <= -1 OR yield_curve_signal <= -1)
            THEN 'Contraction'

            -- RECOVERY: Growth turning positive, employment still weak but improving
            WHEN growth_signal >= 0
                AND employment_signal <= 0
                AND (leading_3mo > 0 OR mfg_signal >= 0)
            THEN 'Recovery'

            -- Default to expansion if signals are mixed but generally positive
            WHEN growth_signal + employment_signal + COALESCE(mfg_signal, 0) > 0
            THEN 'Expansion'

            -- Default to slowdown if unclear
            ELSE 'Slowdown'
        END AS regime,

        -- Confidence level based on signal agreement
        CASE
            WHEN ABS(growth_signal) = 2 AND ABS(employment_signal) >= 1
                AND (growth_signal * employment_signal > 0)  -- Same direction
            THEN 'High'
            WHEN growth_signal != 0 AND employment_signal != 0
                AND (growth_signal * employment_signal > 0)
            THEN 'Medium'
            ELSE 'Low'
        END AS confidence

    FROM regime_signals
)

SELECT
    month_date,
    regime,
    confidence,
    ROUND(composite_score, 2) AS composite_score,

    -- Individual signals for transparency
    growth_signal,
    employment_signal,
    inflation_signal,
    yield_curve_signal,
    financial_signal,
    mfg_signal,

    -- Key indicator values
    ROUND(industrial_production, 2) AS industrial_production,
    ROUND(unemployment_rate, 2) AS unemployment_rate,
    ROUND(nonfarm_payrolls, 0) AS nonfarm_payrolls,
    ROUND(cpi_yoy, 2) AS inflation_yoy,
    ROUND(yield_curve_10y2y, 2) AS yield_curve_spread,
    ROUND(leading_index, 2) AS leading_index,
    ROUND(mfg_production, 1) AS mfg_production,
    ROUND(consumer_sentiment, 1) AS consumer_sentiment,
    ROUND(financial_conditions, 2) AS financial_conditions,

    -- Trend indicators
    ROUND(indpro_3mo, 2) AS indpro_3mo_change,
    ROUND(unrate_3mo, 2) AS unrate_3mo_change,
    ROUND(payrolls_3mo, 0) AS payrolls_3mo_change,

    -- Regime duration tracking
    CASE
        WHEN LAG(regime) OVER (ORDER BY month_date) != regime THEN 1
        ELSE 0
    END AS regime_change_flag,

    -- Previous regime for transition analysis
    LAG(regime) OVER (ORDER BY month_date) AS previous_regime

FROM regime_classification
WHERE regime IS NOT NULL
ORDER BY month_date DESC
