{{
  config(
    description='Analysis of economic data rate of change vs future stock returns, providing correlation analysis and quintile performance insights'
  )
}}

-- Analysis of Economic Data Rate of Change vs Stock Returns
-- Note: Uses trailing returns (pct_change_Xmo) as proxies for forward-looking analysis
WITH economic_changes AS (
    SELECT
        bha.symbol,
        bha.date,
        bha.series_name,
        bha.category,
        fsm.category AS economic_category,
        bha.value AS current_econ_value,
        bha.current_price,
        bha.pct_change_3mo AS quarterly_return_pct,
        bha.pct_change_3mo AS pct_change_q1,
        bha.pct_change_6mo AS pct_change_q2,
        bha.pct_change_9mo AS pct_change_q3,

        -- Calculate month-over-month change in economic data
        LAG(bha.value, 1)
            OVER (
                PARTITION BY bha.symbol, bha.series_name ORDER BY bha.date
            )
            AS prev_econ_value,

        -- Calculate rate of change (month-over-month %)
        CASE
            WHEN
                LAG(bha.value, 1)
                    OVER (
                        PARTITION BY bha.symbol, bha.series_name
                        ORDER BY bha.date
                    )
                IS NOT NULL
                AND LAG(bha.value, 1)
                    OVER (
                        PARTITION BY bha.symbol, bha.series_name
                        ORDER BY bha.date
                    )
                != 0
                THEN (
                    (
                        bha.value
                        - LAG(bha.value, 1)
                            OVER (
                                PARTITION BY bha.symbol, bha.series_name
                                ORDER BY bha.date
                            )
                    )
                    / LAG(bha.value, 1)
                        OVER (
                            PARTITION BY bha.symbol, bha.series_name
                            ORDER BY bha.date
                        )
                ) * 100
        END AS econ_mom_change_pct,

        -- Calculate 3-month rolling average of economic change
        AVG(bha.value) OVER (
            PARTITION BY bha.symbol, bha.series_name
            ORDER BY bha.date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS econ_3mo_avg

    FROM {{ ref('base_historical_analysis') }} AS bha
    LEFT JOIN {{ ref('fred_series_mapping') }} AS fsm
        ON bha.series_name = fsm.series_name
    WHERE bha.value IS NOT NULL
      AND bha.series_name IS NOT NULL
      AND fsm.category IS NOT NULL
),

correlation_analysis AS (
    SELECT
        symbol,
        series_name,
        category,
        economic_category,

        -- Basic correlation metrics between economic change and returns
        COUNT(*) AS observation_count,

        -- Correlation between economic MoM change and returns
        CORR(econ_mom_change_pct, pct_change_q1)
            AS corr_econ_q1_returns,
        CORR(econ_mom_change_pct, pct_change_q2)
            AS corr_econ_q2_returns,
        CORR(econ_mom_change_pct, pct_change_q3)
            AS corr_econ_q3_returns,

        -- Correlation between economic MoM change and quarterly returns
        CORR(econ_mom_change_pct, quarterly_return_pct)
            AS corr_econ_quarterly_total_return,

        -- Average returns when economic data is growing vs declining
        AVG(CASE WHEN econ_mom_change_pct > 0 THEN pct_change_q1 END)
            AS avg_q1_return_when_econ_growing,
        AVG(CASE WHEN econ_mom_change_pct < 0 THEN pct_change_q1 END)
            AS avg_q1_return_when_econ_declining,

        -- Average quarterly returns when economic data is growing vs declining
        AVG(CASE WHEN econ_mom_change_pct > 0 THEN quarterly_return_pct END)
            AS avg_quarterly_total_return_when_econ_growing,
        AVG(CASE WHEN econ_mom_change_pct < 0 THEN quarterly_return_pct END)
            AS avg_quarterly_total_return_when_econ_declining,

        -- Standard deviations
        STDDEV(econ_mom_change_pct) AS econ_change_volatility,
        STDDEV(pct_change_q1) AS q1_return_volatility,

        -- Economic data statistics
        AVG(econ_mom_change_pct) AS avg_econ_change_pct,
        MIN(econ_mom_change_pct) AS min_econ_change_pct,
        MAX(econ_mom_change_pct) AS max_econ_change_pct

    FROM economic_changes
    WHERE econ_mom_change_pct IS NOT NULL
    GROUP BY symbol, series_name, category, economic_category
),

detailed_monthly_view AS (
    SELECT
        symbol,
        date,
        series_name,
        category,
        economic_category,
        econ_mom_change_pct,
        quarterly_return_pct,
        pct_change_q1,
        pct_change_q2,
        pct_change_q3,

        -- Quintile ranking of economic changes
        NTILE(5)
            OVER (PARTITION BY symbol, series_name ORDER BY econ_mom_change_pct)
            AS econ_change_quintile,

        -- Lead/lag analysis - how does economic data relate to past and future returns
        LAG(pct_change_q1, 1)
            OVER (PARTITION BY symbol ORDER BY date)
            AS prev_month_q1_return,
        LEAD(pct_change_q1, 1)
            OVER (PARTITION BY symbol ORDER BY date)
            AS next_month_q1_return

    FROM economic_changes
    WHERE econ_mom_change_pct IS NOT NULL
)

-- Main Results: Show correlations and key insights
SELECT
    'Correlation Analysis' AS analysis_type,
    symbol,
    series_name,
    category,
    economic_category,
    observation_count,
    ROUND(corr_econ_q1_returns, 4) AS correlation_econ_vs_q1_returns,
    ROUND(corr_econ_q2_returns, 4) AS correlation_econ_vs_q2_returns,
    ROUND(corr_econ_q3_returns, 4) AS correlation_econ_vs_q3_returns,
    ROUND(corr_econ_quarterly_total_return, 4) AS correlation_econ_vs_quarterly_total_return,
    ROUND(avg_q1_return_when_econ_growing, 2) AS avg_q1_return_econ_up,
    ROUND(avg_q1_return_when_econ_declining, 2) AS avg_q1_return_econ_down,
    ROUND(avg_quarterly_total_return_when_econ_growing, 2) AS avg_quarterly_total_return_econ_up,
    ROUND(avg_quarterly_total_return_when_econ_declining, 2) AS avg_quarterly_total_return_econ_down,
    ROUND(
        COALESCE(avg_q1_return_when_econ_growing, 0) - COALESCE(avg_q1_return_when_econ_declining, 0),
        2
    ) AS return_difference
FROM correlation_analysis
WHERE observation_count >= 10  -- Filter for meaningful sample sizes

UNION ALL

-- Quintile Analysis: Performance by economic data change quintiles
SELECT
    'Quintile Analysis' AS analysis_type,
    symbol,
    series_name,
    category,
    economic_category,
    NULL AS observation_count,
    econ_change_quintile AS correlation_econ_vs_q1_returns,
    NULL AS correlation_econ_vs_q2_returns,
    NULL AS correlation_econ_vs_q3_returns,
    NULL AS correlation_econ_vs_quarterly_total_return,
    ROUND(AVG(pct_change_q1), 2) AS avg_q1_return_econ_up,
    COUNT(*) AS avg_q1_return_econ_down,
    ROUND(AVG(quarterly_return_pct), 2) AS avg_quarterly_total_return_econ_up,
    NULL AS avg_quarterly_total_return_econ_down,
    ROUND(AVG(econ_mom_change_pct), 2) AS return_difference
FROM detailed_monthly_view
GROUP BY symbol, series_name, category, economic_category, econ_change_quintile
HAVING COUNT(*) >= 3
