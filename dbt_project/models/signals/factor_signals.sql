{{ config(
    description='Factor & valuation signals: value/growth and small/large ratios with trend context'
) }}

/*
    Factor Signals Model

    Builds ratio-based factor signals from existing market data:
    - Value vs Growth spread (IWD/IWF)
    - Small vs Large spread (IWM/SPY)

    Source tables: stg_major_indices (daily OHLC for ETFs)
*/

WITH iwd_prices AS (
    SELECT
        date,
        adj_close AS iwd_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'IWD'
      AND adj_close IS NOT NULL
      AND date >= CURRENT_DATE - INTERVAL 3 YEAR
),

iwf_prices AS (
    SELECT
        date,
        adj_close AS iwf_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'IWF'
      AND adj_close IS NOT NULL
      AND date >= CURRENT_DATE - INTERVAL 3 YEAR
),

iwm_prices AS (
    SELECT
        date,
        adj_close AS iwm_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'IWM'
      AND adj_close IS NOT NULL
      AND date >= CURRENT_DATE - INTERVAL 3 YEAR
),

spy_prices AS (
    SELECT
        date,
        adj_close AS spy_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'SPY'
      AND adj_close IS NOT NULL
      AND date >= CURRENT_DATE - INTERVAL 3 YEAR
),

value_growth_ratio AS (
    SELECT
        iwd.date,
        iwd.iwd_close,
        iwf.iwf_close,
        CASE
            WHEN iwf.iwf_close > 0 THEN iwd.iwd_close / iwf.iwf_close
        END AS iwd_iwf_ratio
    FROM iwd_prices iwd
    INNER JOIN iwf_prices iwf
        ON iwd.date = iwf.date
),

value_growth_indicators AS (
    SELECT
        date,
        iwd_close,
        iwf_close,
        iwd_iwf_ratio,
        AVG(iwd_iwf_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS iwd_iwf_sma_50,
        AVG(iwd_iwf_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS iwd_iwf_sma_200
    FROM value_growth_ratio
),

small_large_ratio AS (
    SELECT
        s.date,
        CASE
            WHEN s.spy_close > 0 THEN i.iwm_close / s.spy_close
        END AS iwm_spy_ratio
    FROM spy_prices s
    INNER JOIN iwm_prices i
        ON s.date = i.date
),

small_large_indicators AS (
    SELECT
        date,
        iwm_spy_ratio,
        AVG(iwm_spy_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS iwm_spy_sma_50,
        AVG(iwm_spy_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS iwm_spy_sma_200
    FROM small_large_ratio
)

SELECT
    vg.date,
    vg.iwd_close,
    vg.iwf_close,
    vg.iwd_iwf_ratio,
    vg.iwd_iwf_sma_50,
    vg.iwd_iwf_sma_200,
    sl.iwm_spy_ratio,
    sl.iwm_spy_sma_50,
    sl.iwm_spy_sma_200
FROM value_growth_indicators vg
LEFT JOIN small_large_indicators sl
    ON vg.date = sl.date
ORDER BY vg.date
