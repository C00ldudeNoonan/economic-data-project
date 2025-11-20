{{ config(
    materialized='incremental',
    unique_key=['snapshot_date', 'symbol', 'series_name', 'category', 'economic_category'],
    incremental_strategy='delete+insert'
) }}

WITH snapshot_dates AS (
    SELECT DISTINCT
        DATE_TRUNC('month', month_date) AS snapshot_date
    FROM {{ ref('base_historical_analysis') }}
    WHERE month_date >= '2020-01-01'
),

snapshot_base_historical AS (
    SELECT
        sd.snapshot_date,
        bha.*
    FROM {{ ref('base_historical_analysis') }} AS bha
    CROSS JOIN snapshot_dates AS sd
    WHERE bha.month_date <= sd.snapshot_date
),

economic_changes AS (
    SELECT
        snapshot_date,
        symbol,
        month_date,
        bha.series_name,
        bha.category,
        fsm.category AS economic_category,
        value AS current_econ_value,
        monthly_avg_close,
        pct_change_q1_forward,
        pct_change_q2_forward,
        pct_change_q3_forward,
        LAG(value, 1) OVER (
            PARTITION BY snapshot_date, symbol, bha.series_name
            ORDER BY month_date
        ) AS prev_econ_value,
        CASE
            WHEN LAG(value, 1) OVER (
                PARTITION BY snapshot_date, symbol, bha.series_name
                ORDER BY month_date
            ) IS NOT NULL
            AND LAG(value, 1) OVER (
                PARTITION BY snapshot_date, symbol, bha.series_name
                ORDER BY month_date
            ) != 0
            THEN (
                (
                    value
                    - LAG(value, 1) OVER (
                        PARTITION BY snapshot_date, symbol, bha.series_name
                        ORDER BY month_date
                    )
                )
                / LAG(value, 1) OVER (
                    PARTITION BY snapshot_date, symbol, bha.series_name
                    ORDER BY month_date
                )
            ) * 100
        END AS econ_mom_change_pct,
        AVG(value) OVER (
            PARTITION BY snapshot_date, symbol, bha.series_name
            ORDER BY month_date
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS econ_3mo_avg
    FROM snapshot_base_historical AS bha
    LEFT JOIN {{ ref('fred_series_mapping') }} AS fsm
        ON bha.series_name = fsm.series_name
    WHERE bha.value IS NOT NULL
),

correlation_analysis AS (
    SELECT
        snapshot_date,
        symbol,
        series_name,
        category,
        economic_category,
        COUNT(*) AS observation_count,
        CORR(econ_mom_change_pct, pct_change_q1_forward)
            AS corr_econ_q1_returns,
        CORR(econ_mom_change_pct, pct_change_q2_forward)
            AS corr_econ_q2_returns,
        CORR(econ_mom_change_pct, pct_change_q3_forward)
            AS corr_econ_q3_returns,
        AVG(CASE WHEN econ_mom_change_pct > 0 THEN pct_change_q1_forward END)
            AS avg_q1_return_when_econ_growing,
        AVG(CASE WHEN econ_mom_change_pct < 0 THEN pct_change_q1_forward END)
            AS avg_q1_return_when_econ_declining,
        STDDEV(econ_mom_change_pct) AS econ_change_volatility,
        STDDEV(pct_change_q1_forward) AS q1_return_volatility,
        AVG(econ_mom_change_pct) AS avg_econ_change_pct,
        MIN(econ_mom_change_pct) AS min_econ_change_pct,
        MAX(econ_mom_change_pct) AS max_econ_change_pct
    FROM economic_changes
    WHERE econ_mom_change_pct IS NOT NULL
    GROUP BY snapshot_date, symbol, series_name, category, economic_category
)

SELECT
    snapshot_date,
    'Correlation Analysis' AS analysis_type,
    symbol,
    series_name,
    category,
    economic_category,
    observation_count,
    ROUND(corr_econ_q1_returns, 4) AS correlation_econ_vs_q1_returns,
    ROUND(corr_econ_q2_returns, 4) AS correlation_econ_vs_q2_returns,
    ROUND(corr_econ_q3_returns, 4) AS correlation_econ_vs_q3_returns,
    ROUND(avg_q1_return_when_econ_growing, 2) AS avg_q1_return_econ_up,
    ROUND(avg_q1_return_when_econ_declining, 2) AS avg_q1_return_econ_down,
    ROUND(avg_q1_return_when_econ_declining, 2) AS return_difference
FROM correlation_analysis
WHERE observation_count >= 10
ORDER BY snapshot_date DESC, symbol, series_name

