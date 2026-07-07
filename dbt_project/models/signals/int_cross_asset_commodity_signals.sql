{% set as_of_date = var('as_of_date', 'CURRENT_DATE()') %}

WITH gold_prices AS (
    SELECT
        date,
        price AS gold_price
    FROM {{ ref('stg_input_commodities') }}
    WHERE commodity_name = 'gold'
      AND price IS NOT NULL
      AND price > 0
      AND date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
),

gold_real_regression AS (
    SELECT
        g.date,
        g.gold_price,
        r.value AS real_yield_10y,
        AVG(r.value) OVER w AS avg_real_yield,
        AVG(g.gold_price) OVER w AS avg_gold_price,
        AVG(r.value * g.gold_price) OVER w AS avg_xy,
        AVG(r.value * r.value) OVER w AS avg_x2
    FROM gold_prices AS g
    INNER JOIN {{ ref('stg_fred_series') }} AS r
        ON g.date = r.date
    WHERE r.series_code = 'DFII10'
      AND r.value IS NOT NULL
      AND r.date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
    WINDOW w AS (ORDER BY g.date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
),

gold_real_residual AS (
    SELECT
        date,
        gold_price,
        real_yield_10y,
        CASE
            WHEN (avg_x2 - (avg_real_yield * avg_real_yield)) <> 0 THEN
                SAFE_DIVIDE(
                    avg_xy - (avg_real_yield * avg_gold_price),
                    avg_x2 - (avg_real_yield * avg_real_yield)
                )
        END AS beta,
        CASE
            WHEN (avg_x2 - (avg_real_yield * avg_real_yield)) <> 0 THEN
                avg_gold_price
                - SAFE_DIVIDE(
                    avg_xy - (avg_real_yield * avg_gold_price),
                    avg_x2 - (avg_real_yield * avg_real_yield)
                ) * avg_real_yield
        END AS alpha
    FROM gold_real_regression
),

gold_real_zscore AS (
    SELECT
        date,
        gold_price,
        real_yield_10y,
        gold_real_residual,
        CASE
            WHEN residual_std > 0 THEN SAFE_DIVIDE(gold_real_residual - residual_avg, residual_std)
        END AS gold_real_residual_zscore
    FROM (
        SELECT
            date,
            gold_price,
            real_yield_10y,
            CASE
                WHEN beta IS NOT NULL AND alpha IS NOT NULL THEN
                    gold_price - (alpha + beta * real_yield_10y)
            END AS gold_real_residual,
            AVG(CASE WHEN beta IS NOT NULL AND alpha IS NOT NULL THEN gold_price - (alpha + beta * real_yield_10y) END) OVER w AS residual_avg,
            STDDEV_SAMP(CASE WHEN beta IS NOT NULL AND alpha IS NOT NULL THEN gold_price - (alpha + beta * real_yield_10y) END) OVER w AS residual_std
        FROM gold_real_residual
        WINDOW w AS (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
    )
),

copper_gold_yield_corr AS (
    SELECT
        c.date,
        c.copper_gold_ratio,
        t.bc_10year AS treasury_10y_yield,
        CORR(c.copper_gold_ratio, t.bc_10year) OVER (
            ORDER BY c.date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS copper_gold_yield_corr_252d
    FROM (
        SELECT
            g.date,
            SAFE_DIVIDE(c.price, g.gold_price) * 1000 AS copper_gold_ratio
        FROM gold_prices AS g
        INNER JOIN {{ ref('stg_input_commodities') }} AS c
            ON g.date = c.date
        WHERE c.commodity_name = 'copper'
          AND c.price IS NOT NULL
          AND c.price > 0
          AND c.date >= DATE_SUB({{ as_of_date }}, INTERVAL 3 YEAR)
    ) AS c
    INNER JOIN {{ ref('stg_treasury_yields') }} AS t
        ON c.date = SAFE_CAST(t.date AS DATE)
    WHERE t.bc_10year IS NOT NULL
)

SELECT
    g.date,
    g.gold_price,
    g.real_yield_10y,
    g.gold_real_residual,
    g.gold_real_residual_zscore,
    c.copper_gold_ratio,
    c.treasury_10y_yield,
    c.copper_gold_yield_corr_252d
FROM gold_real_zscore AS g
LEFT JOIN copper_gold_yield_corr AS c ON g.date = c.date
