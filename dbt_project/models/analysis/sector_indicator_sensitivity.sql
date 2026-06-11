{{
  config(
    description='Calculates rolling correlations between FRED economic indicators and sector ETF returns with lag analysis'
  )
}}

-- Sector-to-name mapping for readable output
WITH sector_names AS (
    SELECT *
    FROM UNNEST([
        STRUCT('XLK' AS symbol, 'Technology' AS sector_name),
        STRUCT('XLC' AS symbol, 'Communication Services' AS sector_name),
        STRUCT('XLY' AS symbol, 'Consumer Discretionary' AS sector_name),
        STRUCT('XLF' AS symbol, 'Financial' AS sector_name),
        STRUCT('XLI' AS symbol, 'Industrial' AS sector_name),
        STRUCT('XLU' AS symbol, 'Utilities' AS sector_name),
        STRUCT('XLP' AS symbol, 'Consumer Staples' AS sector_name),
        STRUCT('XLRE' AS symbol, 'Real Estate' AS sector_name),
        STRUCT('XLB' AS symbol, 'Materials' AS sector_name),
        STRUCT('XLE' AS symbol, 'Energy' AS sector_name),
        STRUCT('XLV' AS symbol, 'Health Care' AS sector_name)
    ])
),

-- Get monthly sector returns (last trading day of each month)
sector_monthly AS (
    SELECT
        symbol,
        DATE_TRUNC(date, MONTH) AS month_date,
        -- Use the last available data point for each month
        LAST_VALUE(pct_change_1mo) OVER (
            PARTITION BY symbol, DATE_TRUNC(date, MONTH)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS return_1mo,
        LAST_VALUE(pct_change_3mo) OVER (
            PARTITION BY symbol, DATE_TRUNC(date, MONTH)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS return_3mo,
        LAST_VALUE(pct_change_6mo) OVER (
            PARTITION BY symbol, DATE_TRUNC(date, MONTH)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS return_6mo,
        LAST_VALUE(pct_change_1yr) OVER (
            PARTITION BY symbol, DATE_TRUNC(date, MONTH)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS return_12mo,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, DATE_TRUNC(date, MONTH)
            ORDER BY date DESC
        ) AS rn
    FROM {{ ref('us_sector_analysis_return') }}
    WHERE symbol IN ('XLK', 'XLC', 'XLY', 'XLF', 'XLI', 'XLU', 'XLP', 'XLRE', 'XLB', 'XLE', 'XLV')
),

sector_returns AS (
    SELECT
        symbol,
        month_date,
        return_1mo,
        return_3mo,
        return_6mo,
        return_12mo
    FROM sector_monthly
    WHERE rn = 1
),

-- Get monthly FRED indicator values with month-over-month changes
indicator_monthly AS (
    SELECT
        series_code,
        series_name,
        DATE_TRUNC(date, MONTH) AS month_date,
        value,
        -- Calculate month-over-month percentage change
        CASE
            WHEN LAG(value) OVER (PARTITION BY series_code ORDER BY date) IS NOT NULL
                AND LAG(value) OVER (PARTITION BY series_code ORDER BY date) != 0
            THEN ((value - LAG(value) OVER (PARTITION BY series_code ORDER BY date))
                  / ABS(LAG(value) OVER (PARTITION BY series_code ORDER BY date))) * 100
        END AS indicator_mom_pct,
        -- Calculate 3-month change
        CASE
            WHEN LAG(value, 3) OVER (PARTITION BY series_code ORDER BY date) IS NOT NULL
                AND LAG(value, 3) OVER (PARTITION BY series_code ORDER BY date) != 0
            THEN ((value - LAG(value, 3) OVER (PARTITION BY series_code ORDER BY date))
                  / ABS(LAG(value, 3) OVER (PARTITION BY series_code ORDER BY date))) * 100
        END AS indicator_3mo_pct
    FROM {{ ref('fred_monthly_diff') }}
),

indicator_monthly_distinct AS (
    SELECT DISTINCT
        series_code,
        series_name,
        month_date,
        value,
        indicator_mom_pct,
        indicator_3mo_pct
    FROM indicator_monthly
),

-- Join sectors with indicators (including lags)
sector_indicator_joined AS (
    SELECT
        sr.symbol,
        sn.sector_name,
        sr.month_date,
        sr.return_1mo,
        sr.return_3mo,
        sr.return_6mo,
        sr.return_12mo,
        im.series_code,
        im.series_name,
        fsm.category AS indicator_category,
        im.value AS indicator_value,
        im.indicator_mom_pct,
        im.indicator_3mo_pct,
        -- Lagged indicator values (1-6 months prior)
        LAG(im.indicator_mom_pct, 1) OVER (
            PARTITION BY sr.symbol, im.series_code ORDER BY sr.month_date
        ) AS indicator_mom_lag1,
        LAG(im.indicator_mom_pct, 2) OVER (
            PARTITION BY sr.symbol, im.series_code ORDER BY sr.month_date
        ) AS indicator_mom_lag2,
        LAG(im.indicator_mom_pct, 3) OVER (
            PARTITION BY sr.symbol, im.series_code ORDER BY sr.month_date
        ) AS indicator_mom_lag3,
        LAG(im.indicator_mom_pct, 6) OVER (
            PARTITION BY sr.symbol, im.series_code ORDER BY sr.month_date
        ) AS indicator_mom_lag6
    FROM sector_returns sr
    CROSS JOIN indicator_monthly_distinct im
    LEFT JOIN sector_names sn ON sr.symbol = sn.symbol
    LEFT JOIN {{ ref('fred_series_mapping') }} fsm ON im.series_code = fsm.code
    WHERE sr.month_date = im.month_date
),

-- Calculate correlations for each sector-indicator pair
correlation_calcs AS (
    SELECT
        symbol,
        sector_name,
        series_code,
        series_name,
        indicator_category,
        COUNT(*) AS observation_count,

        -- Contemporaneous correlations (indicator change vs sector return)
        ROUND(CORR(indicator_mom_pct, return_1mo), 4) AS corr_1mo_contemp,
        ROUND(CORR(indicator_mom_pct, return_3mo), 4) AS corr_3mo_contemp,
        ROUND(CORR(indicator_mom_pct, return_6mo), 4) AS corr_6mo_contemp,
        ROUND(CORR(indicator_mom_pct, return_12mo), 4) AS corr_12mo_contemp,

        -- Lagged correlations (does indicator lead sector performance?)
        ROUND(CORR(indicator_mom_lag1, return_1mo), 4) AS corr_1mo_lag1,
        ROUND(CORR(indicator_mom_lag2, return_1mo), 4) AS corr_1mo_lag2,
        ROUND(CORR(indicator_mom_lag3, return_1mo), 4) AS corr_1mo_lag3,
        ROUND(CORR(indicator_mom_lag6, return_1mo), 4) AS corr_1mo_lag6,

        -- 3-month indicator change correlations
        ROUND(CORR(indicator_3mo_pct, return_3mo), 4) AS corr_3mo_indicator_3mo_return,

        -- Average returns when indicator is rising vs falling
        ROUND(AVG(CASE WHEN indicator_mom_pct > 0 THEN return_1mo END), 2) AS avg_return_indicator_up,
        ROUND(AVG(CASE WHEN indicator_mom_pct < 0 THEN return_1mo END), 2) AS avg_return_indicator_down,
        ROUND(AVG(CASE WHEN indicator_mom_pct > 0 THEN return_1mo END) -
              AVG(CASE WHEN indicator_mom_pct < 0 THEN return_1mo END), 2) AS return_spread,

        -- Volatility metrics
        ROUND(STDDEV(indicator_mom_pct), 2) AS indicator_volatility,
        ROUND(STDDEV(return_1mo), 2) AS sector_return_volatility

    FROM sector_indicator_joined
    WHERE indicator_mom_pct IS NOT NULL
    GROUP BY symbol, sector_name, series_code, series_name, indicator_category
    HAVING COUNT(*) >= 24  -- Require at least 2 years of data
)

SELECT
    symbol,
    sector_name,
    series_code,
    series_name,
    indicator_category,
    observation_count,

    -- Contemporaneous correlations
    corr_1mo_contemp,
    corr_3mo_contemp,
    corr_6mo_contemp,
    corr_12mo_contemp,

    -- Lagged correlations (predictive power)
    corr_1mo_lag1,
    corr_1mo_lag2,
    corr_1mo_lag3,
    corr_1mo_lag6,

    -- Best lag correlation (highest absolute value among lags)
    CASE
        WHEN ABS(COALESCE(corr_1mo_lag1, 0)) >= ABS(COALESCE(corr_1mo_lag2, 0))
            AND ABS(COALESCE(corr_1mo_lag1, 0)) >= ABS(COALESCE(corr_1mo_lag3, 0))
            AND ABS(COALESCE(corr_1mo_lag1, 0)) >= ABS(COALESCE(corr_1mo_lag6, 0))
        THEN 1
        WHEN ABS(COALESCE(corr_1mo_lag2, 0)) >= ABS(COALESCE(corr_1mo_lag3, 0))
            AND ABS(COALESCE(corr_1mo_lag2, 0)) >= ABS(COALESCE(corr_1mo_lag6, 0))
        THEN 2
        WHEN ABS(COALESCE(corr_1mo_lag3, 0)) >= ABS(COALESCE(corr_1mo_lag6, 0))
        THEN 3
        ELSE 6
    END AS best_lag_months,

    GREATEST(
        ABS(COALESCE(corr_1mo_lag1, 0)),
        ABS(COALESCE(corr_1mo_lag2, 0)),
        ABS(COALESCE(corr_1mo_lag3, 0)),
        ABS(COALESCE(corr_1mo_lag6, 0))
    ) AS best_lag_correlation_abs,

    -- 3-month indicator vs 3-month return correlation
    corr_3mo_indicator_3mo_return,

    -- Return spread analysis
    avg_return_indicator_up,
    avg_return_indicator_down,
    return_spread,

    -- Volatility
    indicator_volatility,
    sector_return_volatility,

    -- Sensitivity score: combination of correlation strength and predictive power
    ROUND(
        (ABS(COALESCE(corr_1mo_contemp, 0)) * 0.3 +
         ABS(COALESCE(corr_3mo_contemp, 0)) * 0.2 +
         GREATEST(
             ABS(COALESCE(corr_1mo_lag1, 0)),
             ABS(COALESCE(corr_1mo_lag2, 0)),
             ABS(COALESCE(corr_1mo_lag3, 0))
         ) * 0.5) * 100,
        2
    ) AS sensitivity_score

FROM correlation_calcs
ORDER BY symbol, sensitivity_score DESC
