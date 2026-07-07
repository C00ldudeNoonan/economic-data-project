{% set as_of_date = var('as_of_date', 'CURRENT_DATE()') %}

WITH spy_prices AS (
    SELECT
        date,
        adj_close AS spy_close,
        MAX(adj_close) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS spy_high_252d
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'SPY'
      AND adj_close IS NOT NULL
      AND date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
),

dow_theory AS (
    SELECT
        d.date,
        d.adj_close AS dia_close,
        i.adj_close AS iyt_close,
        MAX(d.adj_close) OVER (ORDER BY d.date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS dia_high_252d,
        MAX(i.adj_close) OVER (ORDER BY i.date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS iyt_high_252d
    FROM {{ ref('stg_major_indices') }} AS d
    INNER JOIN {{ ref('stg_major_indices') }} AS i
        ON d.date = i.date
    WHERE d.symbol = 'DIA'
      AND i.symbol = 'IYT'
      AND d.adj_close IS NOT NULL
      AND i.adj_close IS NOT NULL
      AND d.date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
),

soxx_spy_indicators AS (
    SELECT
        date,
        soxx_spy_ratio,
        AVG(soxx_spy_ratio) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS soxx_spy_sma_200
    FROM (
        SELECT
            s.date,
            SAFE_DIVIDE(x.adj_close, s.spy_close) AS soxx_spy_ratio
        FROM spy_prices AS s
        INNER JOIN {{ ref('stg_major_indices') }} AS x
            ON s.date = x.date
        WHERE x.symbol = 'SOXX'
          AND x.adj_close IS NOT NULL
          AND x.date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
    )
)

SELECT
    s.date,
    dow.dia_close,
    dow.iyt_close,
    dow.dia_high_252d,
    dow.iyt_high_252d,
    CASE
        WHEN dow.dia_close >= dow.dia_high_252d AND dow.iyt_close < dow.iyt_high_252d * 0.98 THEN 1
        ELSE 0
    END AS dow_non_confirmation_flag,
    soxx.soxx_spy_ratio,
    soxx.soxx_spy_sma_200,
    CASE
        WHEN soxx.soxx_spy_ratio < soxx.soxx_spy_sma_200 AND s.spy_close >= s.spy_high_252d * 0.98 THEN 1
        ELSE 0
    END AS semis_divergence_flag
FROM spy_prices AS s
LEFT JOIN dow_theory AS dow ON s.date = dow.date
LEFT JOIN soxx_spy_indicators AS soxx ON s.date = soxx.date
