{% set as_of_date = var('as_of_date', 'CURRENT_DATE()') %}

WITH spy_prices AS (
    SELECT
        date,
        adj_close AS spy_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'SPY'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
),

spy_indicators AS (
    SELECT
        date,
        spy_close,
        AVG(spy_close) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS spy_sma_50,
        AVG(spy_close) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS spy_sma_200,
        MAX(spy_close) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS spy_high_252d
    FROM spy_prices
),

hyg_indicators AS (
    SELECT
        date,
        adj_close AS hyg_close,
        AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS hyg_sma_50
    FROM {{ ref('stg_fixed_income') }}
    WHERE symbol = 'HYG'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
),

hy_spread_indicators AS (
    SELECT
        date,
        value AS hy_spread,
        value - LAG(value, 20) OVER (ORDER BY date) AS hy_spread_20d_change
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'BAMLH0A0HYM2'
      AND value IS NOT NULL
      AND date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
),

hy_equity_divergence AS (
    SELECT
        s.date,
        s.spy_close,
        s.spy_sma_50,
        s.spy_sma_200,
        s.spy_high_252d,
        h.hyg_close,
        h.hyg_sma_50,
        hs.hy_spread,
        hs.hy_spread_20d_change,
        CASE
            WHEN h.hyg_close < h.hyg_sma_50 AND s.spy_close > s.spy_sma_50 THEN 1
            ELSE 0
        END AS hy_equity_divergence_flag,
        CASE
            WHEN hs.hy_spread_20d_change > 0 AND s.spy_close >= s.spy_high_252d THEN 1
            ELSE 0
        END AS hy_spread_divergence_flag
    FROM spy_indicators AS s
    LEFT JOIN hyg_indicators AS h ON s.date = h.date
    LEFT JOIN hy_spread_indicators AS hs ON s.date = hs.date
),

spy_returns AS (
    SELECT
        date,
        SAFE_DIVIDE(spy_close, LAG(spy_close) OVER (ORDER BY date)) - 1.0 AS spy_return
    FROM spy_prices
),

govt_returns AS (
    SELECT
        date,
        SAFE_DIVIDE(adj_close, LAG(adj_close) OVER (ORDER BY date)) - 1.0 AS govt_return
    FROM {{ ref('stg_fixed_income') }}
    WHERE symbol = 'GOVT'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
),

stock_bond_corr AS (
    SELECT
        s.date,
        CORR(s.spy_return, g.govt_return) OVER (
            ORDER BY s.date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS stock_bond_corr_252d
    FROM spy_returns AS s
    INNER JOIN govt_returns AS g ON s.date = g.date
    WHERE s.spy_return IS NOT NULL
      AND g.govt_return IS NOT NULL
)

SELECT
    h.*,
    sb.stock_bond_corr_252d,
    CASE
        WHEN sb.stock_bond_corr_252d > 0 THEN 'positive'
        WHEN sb.stock_bond_corr_252d IS NULL THEN NULL
        ELSE 'negative'
    END AS stock_bond_corr_regime
FROM hy_equity_divergence AS h
LEFT JOIN stock_bond_corr AS sb ON h.date = sb.date
