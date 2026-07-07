{% set as_of_date = var('as_of_date', 'CURRENT_DATE()') %}

WITH spy_prices AS (
    SELECT
        date,
        adj_close AS spy_close,
        AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS spy_sma_50
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'SPY'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
),

defensive_indicators AS (
    SELECT
        date,
        xlp_xly_ratio,
        AVG(xlp_xly_ratio) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS xlp_xly_sma_50,
        AVG(xlp_xly_ratio) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS xlp_xly_sma_200
    FROM (
        SELECT
            xlp.date,
            SAFE_DIVIDE(xlp.adj_close, xly.adj_close) AS xlp_xly_ratio
        FROM {{ ref('stg_us_sectors') }} AS xlp
        INNER JOIN {{ ref('stg_us_sectors') }} AS xly
            ON xlp.date = xly.date
        WHERE xlp.symbol = 'XLP'
          AND xly.symbol = 'XLY'
          AND xlp.adj_close IS NOT NULL
          AND xly.adj_close IS NOT NULL
          AND xlp.date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
    )
),

fxa_spy_indicators AS (
    SELECT
        date,
        fxa_spy_ratio,
        AVG(fxa_spy_ratio) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS fxa_spy_sma_50
    FROM (
        SELECT
            s.date,
            SAFE_DIVIDE(f.adj_close, s.spy_close) AS fxa_spy_ratio
        FROM spy_prices AS s
        INNER JOIN {{ ref('stg_currency') }} AS f
            ON s.date = f.date
        WHERE f.symbol = 'FXA'
          AND f.adj_close IS NOT NULL
          AND f.date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
    )
)

SELECT
    s.date,
    d.xlp_xly_ratio,
    d.xlp_xly_sma_50,
    d.xlp_xly_sma_200,
    CASE
        WHEN d.xlp_xly_ratio > d.xlp_xly_sma_50 AND d.xlp_xly_sma_50 > d.xlp_xly_sma_200 THEN 1
        ELSE 0
    END AS defensive_ratio_uptrend_flag,
    f.fxa_spy_ratio,
    f.fxa_spy_sma_50,
    CASE
        WHEN f.fxa_spy_ratio < f.fxa_spy_sma_50 AND s.spy_close > s.spy_sma_50 THEN 1
        ELSE 0
    END AS aud_risk_divergence_flag
FROM spy_prices AS s
LEFT JOIN defensive_indicators AS d ON s.date = d.date
LEFT JOIN fxa_spy_indicators AS f ON s.date = f.date
