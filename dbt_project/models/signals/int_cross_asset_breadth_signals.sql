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

iwm_spy_indicators AS (
    SELECT
        date,
        iwm_spy_ratio,
        AVG(iwm_spy_ratio) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS iwm_spy_sma_50,
        AVG(iwm_spy_ratio) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS iwm_spy_sma_200
    FROM (
        SELECT
            s.date,
            SAFE_DIVIDE(i.adj_close, s.spy_close) AS iwm_spy_ratio
        FROM spy_prices AS s
        INNER JOIN {{ ref('stg_major_indices') }} AS i
            ON s.date = i.date
        WHERE i.symbol = 'IWM'
          AND i.adj_close IS NOT NULL
          AND i.date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
    )
),

rsp_spy_indicators AS (
    SELECT
        date,
        rsp_spy_ratio,
        AVG(rsp_spy_ratio) OVER (ORDER BY date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS rsp_spy_sma_50,
        AVG(rsp_spy_ratio) OVER (ORDER BY date ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS rsp_spy_sma_200
    FROM (
        SELECT
            s.date,
            SAFE_DIVIDE(r.adj_close, s.spy_close) AS rsp_spy_ratio
        FROM spy_prices AS s
        INNER JOIN {{ ref('stg_major_indices') }} AS r
            ON s.date = r.date
        WHERE r.symbol = 'RSP'
          AND r.adj_close IS NOT NULL
          AND r.date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
    )
)

SELECT
    i.date,
    i.iwm_spy_ratio,
    i.iwm_spy_sma_50,
    i.iwm_spy_sma_200,
    r.rsp_spy_ratio,
    r.rsp_spy_sma_50,
    r.rsp_spy_sma_200
FROM iwm_spy_indicators AS i
LEFT JOIN rsp_spy_indicators AS r ON i.date = r.date
