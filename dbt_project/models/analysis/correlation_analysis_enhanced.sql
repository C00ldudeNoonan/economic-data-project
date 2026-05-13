{{
  config(
    description='Enhanced correlation analysis with statistical significance, rolling stability, and regime breakdown'
  )
}}

-- Enhanced Correlation Analysis
-- Adds: t-statistics, p-value approximation, rolling stability, regime breakdown

WITH sector_monthly AS (
    SELECT
        symbol,
        DATE_TRUNC('month', date) AS month_date,
        LAST_VALUE(pct_change_1mo) OVER (
            PARTITION BY symbol, DATE_TRUNC('month', date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS monthly_return,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, DATE_TRUNC('month', date)
            ORDER BY date DESC
        ) AS rn
    FROM {{ ref('us_sector_analysis_return') }}
    WHERE symbol IN ('XLK', 'XLC', 'XLY', 'XLF', 'XLI', 'XLU', 'XLP', 'XLRE', 'XLB', 'XLE', 'XLV')
),

sector_returns AS (
    SELECT symbol, month_date, monthly_return
    FROM sector_monthly
    WHERE rn = 1
),

indicator_monthly AS (
    SELECT
        series_code,
        series_name,
        DATE_TRUNC('month', date) AS month_date,
        value,
        CASE
            WHEN LAG(value) OVER (PARTITION BY series_code ORDER BY date) IS NOT NULL
                AND LAG(value) OVER (PARTITION BY series_code ORDER BY date) != 0
            THEN ((value - LAG(value) OVER (PARTITION BY series_code ORDER BY date))
                  / ABS(LAG(value) OVER (PARTITION BY series_code ORDER BY date))) * 100
        END AS indicator_mom_pct
    FROM {{ ref('fred_monthly_diff') }}
),

indicator_monthly_distinct AS (
    SELECT DISTINCT
        series_code,
        series_name,
        month_date,
        indicator_mom_pct
    FROM indicator_monthly
),

-- Join with regime classification
sector_indicator_regime AS (
    SELECT
        sr.symbol,
        sr.month_date,
        sr.monthly_return,
        im.series_code,
        im.series_name,
        im.indicator_mom_pct,
        fsm.category AS indicator_category,
        COALESCE(rc.regime, 'Unknown') AS regime
    FROM sector_returns sr
    CROSS JOIN indicator_monthly_distinct im
    LEFT JOIN {{ ref('fred_series_mapping') }} fsm ON im.series_code = fsm.code
    LEFT JOIN {{ ref('economic_regime_classification') }} rc ON sr.month_date = rc.month_date
    WHERE sr.month_date = im.month_date
        AND im.indicator_mom_pct IS NOT NULL
        AND sr.monthly_return IS NOT NULL
),

-- Calculate overall correlations with statistics
overall_correlations AS (
    SELECT
        symbol,
        series_code,
        series_name,
        indicator_category,
        COUNT(*) AS n_observations,

        -- Correlation coefficient
        CORR(indicator_mom_pct, monthly_return) AS correlation,

        -- Mean and std for both variables (needed for t-stat)
        AVG(indicator_mom_pct) AS mean_indicator,
        AVG(monthly_return) AS mean_return,
        STDDEV(indicator_mom_pct) AS std_indicator,
        STDDEV(monthly_return) AS std_return,

        -- Return spread
        AVG(CASE WHEN indicator_mom_pct > 0 THEN monthly_return END) AS avg_return_indicator_up,
        AVG(CASE WHEN indicator_mom_pct < 0 THEN monthly_return END) AS avg_return_indicator_down

    FROM sector_indicator_regime
    GROUP BY symbol, series_code, series_name, indicator_category
    HAVING COUNT(*) >= 24  -- At least 2 years of data
),

-- Calculate correlations by regime
regime_correlations AS (
    SELECT
        symbol,
        series_code,
        series_name,
        regime,
        COUNT(*) AS n_observations,
        CORR(indicator_mom_pct, monthly_return) AS correlation,
        AVG(CASE WHEN indicator_mom_pct > 0 THEN monthly_return END) AS avg_return_indicator_up,
        AVG(CASE WHEN indicator_mom_pct < 0 THEN monthly_return END) AS avg_return_indicator_down
    FROM sector_indicator_regime
    WHERE regime IN ('Expansion', 'Slowdown', 'Contraction', 'Recovery')
    GROUP BY symbol, series_code, series_name, regime
    HAVING COUNT(*) >= 6  -- At least 6 months per regime
),

-- Pivot regime correlations
regime_pivot AS (
    SELECT
        symbol,
        series_code,
        series_name,
        MAX(CASE WHEN regime = 'Expansion' THEN correlation END) AS corr_expansion,
        MAX(CASE WHEN regime = 'Slowdown' THEN correlation END) AS corr_slowdown,
        MAX(CASE WHEN regime = 'Contraction' THEN correlation END) AS corr_contraction,
        MAX(CASE WHEN regime = 'Recovery' THEN correlation END) AS corr_recovery,
        MAX(CASE WHEN regime = 'Expansion' THEN n_observations END) AS n_expansion,
        MAX(CASE WHEN regime = 'Slowdown' THEN n_observations END) AS n_slowdown,
        MAX(CASE WHEN regime = 'Contraction' THEN n_observations END) AS n_contraction,
        MAX(CASE WHEN regime = 'Recovery' THEN n_observations END) AS n_recovery
    FROM regime_correlations
    GROUP BY symbol, series_code, series_name
),

-- Calculate rolling correlations for stability analysis
rolling_correlations AS (
    SELECT
        symbol,
        series_code,
        series_name,
        month_date,
        -- 12-month rolling correlation
        CORR(indicator_mom_pct, monthly_return) OVER (
            PARTITION BY symbol, series_code
            ORDER BY month_date
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS rolling_corr_12m,
        -- 24-month rolling correlation
        CORR(indicator_mom_pct, monthly_return) OVER (
            PARTITION BY symbol, series_code
            ORDER BY month_date
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) AS rolling_corr_24m
    FROM sector_indicator_regime
),

-- Calculate correlation stability metrics
correlation_stability AS (
    SELECT
        symbol,
        series_code,
        series_name,
        -- Stability = 1 - coefficient of variation of rolling correlation
        STDDEV(rolling_corr_12m) AS rolling_corr_std,
        AVG(rolling_corr_12m) AS rolling_corr_mean,
        MIN(rolling_corr_12m) AS rolling_corr_min,
        MAX(rolling_corr_12m) AS rolling_corr_max,
        -- Count sign changes (correlation instability indicator)
        SUM(CASE
            WHEN rolling_corr_12m * LAG(rolling_corr_12m) OVER (
                PARTITION BY symbol, series_code ORDER BY month_date
            ) < 0 THEN 1 ELSE 0
        END) AS sign_changes
    FROM rolling_correlations
    WHERE rolling_corr_12m IS NOT NULL
    GROUP BY symbol, series_code, series_name
),

-- Final combined analysis
final_analysis AS (
    SELECT
        oc.symbol,
        oc.series_code,
        oc.series_name,
        oc.indicator_category,
        oc.n_observations,

        -- Overall correlation
        ROUND(oc.correlation, 4) AS correlation,

        -- T-statistic: t = r * sqrt(n-2) / sqrt(1-r^2)
        ROUND(
            CASE
                WHEN ABS(oc.correlation) < 0.9999 AND oc.n_observations > 2
                THEN oc.correlation * SQRT(oc.n_observations - 2) / SQRT(1 - POWER(oc.correlation, 2))
            END,
            3
        ) AS t_statistic,

        -- Approximate p-value based on t-distribution (using normal approximation for large n)
        -- For |t| > 1.96, p < 0.05; for |t| > 2.576, p < 0.01
        CASE
            WHEN ABS(oc.correlation * SQRT(oc.n_observations - 2) / NULLIF(SQRT(1 - POWER(oc.correlation, 2)), 0)) > 3.291 THEN 'p < 0.001'
            WHEN ABS(oc.correlation * SQRT(oc.n_observations - 2) / NULLIF(SQRT(1 - POWER(oc.correlation, 2)), 0)) > 2.576 THEN 'p < 0.01'
            WHEN ABS(oc.correlation * SQRT(oc.n_observations - 2) / NULLIF(SQRT(1 - POWER(oc.correlation, 2)), 0)) > 1.96 THEN 'p < 0.05'
            WHEN ABS(oc.correlation * SQRT(oc.n_observations - 2) / NULLIF(SQRT(1 - POWER(oc.correlation, 2)), 0)) > 1.645 THEN 'p < 0.10'
            ELSE 'p >= 0.10'
        END AS significance_level,

        -- Is statistically significant at 5%?
        CASE
            WHEN ABS(oc.correlation * SQRT(oc.n_observations - 2) / NULLIF(SQRT(1 - POWER(oc.correlation, 2)), 0)) > 1.96
            THEN TRUE ELSE FALSE
        END AS is_significant,

        -- Return spread
        ROUND(oc.avg_return_indicator_up, 2) AS avg_return_indicator_up,
        ROUND(oc.avg_return_indicator_down, 2) AS avg_return_indicator_down,
        ROUND(COALESCE(oc.avg_return_indicator_up, 0) - COALESCE(oc.avg_return_indicator_down, 0), 2) AS return_spread,

        -- Regime correlations
        ROUND(rp.corr_expansion, 4) AS corr_expansion,
        ROUND(rp.corr_slowdown, 4) AS corr_slowdown,
        ROUND(rp.corr_contraction, 4) AS corr_contraction,
        ROUND(rp.corr_recovery, 4) AS corr_recovery,

        -- Regime sample sizes
        rp.n_expansion,
        rp.n_slowdown,
        rp.n_contraction,
        rp.n_recovery,

        -- Correlation stability metrics
        ROUND(cs.rolling_corr_std, 4) AS correlation_volatility,
        ROUND(cs.rolling_corr_min, 4) AS correlation_min,
        ROUND(cs.rolling_corr_max, 4) AS correlation_max,
        cs.sign_changes AS correlation_sign_changes,

        -- Stability score: lower is more stable
        ROUND(
            CASE
                WHEN cs.rolling_corr_mean != 0
                THEN ABS(cs.rolling_corr_std / cs.rolling_corr_mean)
                ELSE NULL
            END,
            2
        ) AS stability_score,

        -- Is correlation stable? (coefficient of variation < 1)
        CASE
            WHEN cs.rolling_corr_mean != 0
                AND ABS(cs.rolling_corr_std / cs.rolling_corr_mean) < 1
            THEN TRUE ELSE FALSE
        END AS is_stable

    FROM overall_correlations oc
    LEFT JOIN regime_pivot rp ON oc.symbol = rp.symbol AND oc.series_code = rp.series_code
    LEFT JOIN correlation_stability cs ON oc.symbol = cs.symbol AND oc.series_code = cs.series_code
)

SELECT
    symbol,
    series_code,
    series_name,
    indicator_category,
    n_observations,
    correlation,
    t_statistic,
    significance_level,
    is_significant,
    avg_return_indicator_up,
    avg_return_indicator_down,
    return_spread,
    corr_expansion,
    corr_slowdown,
    corr_contraction,
    corr_recovery,
    n_expansion,
    n_slowdown,
    n_contraction,
    n_recovery,
    correlation_volatility,
    correlation_min,
    correlation_max,
    correlation_sign_changes,
    stability_score,
    is_stable,

    -- Quality score combining significance and stability
    CASE
        WHEN is_significant AND is_stable THEN 'High'
        WHEN is_significant OR is_stable THEN 'Medium'
        ELSE 'Low'
    END AS quality_rating

FROM final_analysis
ORDER BY symbol, ABS(correlation) DESC
