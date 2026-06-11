/*
    Market Volatility Signals Model

    Computes volatility metrics for SPY and QQQ along with VIX:
    - Realized volatility (20/30 day, annualized)
    - Variance Risk Premium (VIX - realized vol)
    - Parkinson and Garman-Klass estimators (20/60 day, annualized)
*/

WITH vix_data AS (
    SELECT
        date,
        value AS vix_close
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'VIXCLS'
),

vix_stats AS (
    SELECT
        date,
        vix_close,
        AVG(vix_close) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vix_avg_20d,
        MIN(vix_close) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vix_min_20d,
        MAX(vix_close) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vix_max_20d,
        LAG(vix_close) OVER (ORDER BY date) AS vix_prev_close
    FROM vix_data
),

price_base AS (
    SELECT
        symbol,
        date,
        adj_open,
        adj_high,
        adj_low,
        adj_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol IN ('SPY', 'QQQ')
      AND adj_close IS NOT NULL
),

returns AS (
    SELECT
        symbol,
        date,
        adj_open,
        adj_high,
        adj_low,
        adj_close,
        (SAFE_DIVIDE(adj_close, LAG(adj_close) OVER (PARTITION BY symbol ORDER BY date)) - 1.0) AS daily_return
    FROM price_base
),

vol_inputs AS (
    SELECT
        symbol,
        date,
        daily_return,
        STDDEV_SAMP(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) * SQRT(252) * 100 AS realized_vol_20d,
        STDDEV_SAMP(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) * SQRT(252) * 100 AS realized_vol_30d,
        LN(SAFE_DIVIDE(adj_high, adj_low)) AS log_hl,
        LN(SAFE_DIVIDE(adj_close, adj_open)) AS log_co
    FROM returns
    WHERE adj_high > 0
      AND adj_low > 0
      AND adj_open > 0
      AND adj_close > 0
      AND daily_return IS NOT NULL
),

parkinson_gk AS (
    SELECT
        symbol,
        date,
        realized_vol_20d,
        realized_vol_30d,
        (SUM(POWER(log_hl, 2)) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) / (4.0 * 20 * LN(2))) AS parkinson_var_20d,
        (SUM(POWER(log_hl, 2)) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
        ) / (4.0 * 60 * LN(2))) AS parkinson_var_60d,
        (0.5 * POWER(log_hl, 2) - (2 * LN(2) - 1) * POWER(log_co, 2)) AS gk_component
    FROM vol_inputs
),

vol_estimates AS (
    SELECT
        symbol,
        date,
        realized_vol_20d,
        realized_vol_30d,
        SQRT(parkinson_var_20d * 252) * 100 AS parkinson_vol_20d,
        SQRT(parkinson_var_60d * 252) * 100 AS parkinson_vol_60d,
        SQRT(
            (SUM(gk_component) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) / 20.0) * 252
        ) * 100 AS gk_vol_20d,
        SQRT(
            (SUM(gk_component) OVER (
                PARTITION BY symbol
                ORDER BY date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) / 60.0) * 252
        ) * 100 AS gk_vol_60d
    FROM parkinson_gk
),

spy AS (
    SELECT *
    FROM vol_estimates
    WHERE symbol = 'SPY'
),

qqq AS (
    SELECT *
    FROM vol_estimates
    WHERE symbol = 'QQQ'
)

SELECT
    v.date,
    v.vix_close,
    v.vix_avg_20d,
    v.vix_min_20d,
    v.vix_max_20d,
    v.vix_prev_close,
    v.vix_close - v.vix_prev_close AS vix_daily_change,
    CASE
        WHEN v.vix_prev_close > 0 THEN SAFE_DIVIDE(v.vix_close - v.vix_prev_close, v.vix_prev_close) * 100
        ELSE 0
    END AS vix_daily_change_pct,
    spy.realized_vol_20d AS spy_realized_vol_20d,
    spy.realized_vol_30d AS spy_realized_vol_30d,
    spy.parkinson_vol_20d AS spy_parkinson_vol_20d,
    spy.parkinson_vol_60d AS spy_parkinson_vol_60d,
    spy.gk_vol_20d AS spy_gk_vol_20d,
    spy.gk_vol_60d AS spy_gk_vol_60d,
    qqq.realized_vol_20d AS qqq_realized_vol_20d,
    qqq.realized_vol_30d AS qqq_realized_vol_30d,
    qqq.parkinson_vol_20d AS qqq_parkinson_vol_20d,
    qqq.parkinson_vol_60d AS qqq_parkinson_vol_60d,
    qqq.gk_vol_20d AS qqq_gk_vol_20d,
    qqq.gk_vol_60d AS qqq_gk_vol_60d,
    v.vix_close - spy.realized_vol_20d AS spy_vrp_20d,
    v.vix_close - spy.realized_vol_30d AS spy_vrp_30d,
    v.vix_close - qqq.realized_vol_20d AS qqq_vrp_20d,
    v.vix_close - qqq.realized_vol_30d AS qqq_vrp_30d
FROM vix_stats v
LEFT JOIN spy ON v.date = spy.date
LEFT JOIN qqq ON v.date = qqq.date
WHERE v.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
ORDER BY v.date DESC
