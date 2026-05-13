{{ config(
    materialized='view'
) }}

/*
    Commodity Market Signals Model

    Calculates market health ratios using commodity and S&P 500 data:
    - Copper/Gold ratio: Economic health proxy (higher = risk-on, growth expectations)
    - Gold/SPY ratio: Risk sentiment (higher = risk-off, flight to safety)
    - Oil momentum: Trend indicator (current vs moving averages)

    Used by market signals API for Phase 5 indicators.
*/

WITH gold_prices AS (
    SELECT
        date,
        price AS gold_price
    FROM {{ ref('stg_input_commodities') }}
    WHERE
        commodity_name = 'gold'
        AND price IS NOT NULL
        AND price > 0
),

copper_prices AS (
    SELECT
        date,
        price AS copper_price
    FROM {{ ref('stg_input_commodities') }}
    WHERE
        commodity_name = 'copper'
        AND price IS NOT NULL
        AND price > 0
),

oil_prices AS (
    SELECT
        date,
        price AS oil_price
    FROM {{ ref('stg_energy_commodities') }}
    WHERE
        commodity_name = 'crude_oil'
        AND price IS NOT NULL
        AND price > 0
),

spy_prices AS (
    SELECT
        date,
        adj_close AS spy_price
    FROM {{ ref('stg_major_indices') }}
    WHERE
        symbol = 'SPY'
        AND adj_close IS NOT NULL
        AND adj_close > 0
),

-- Get all unique dates where we have at least gold or SPY data
all_dates AS (
    SELECT DISTINCT date FROM gold_prices
    UNION
    SELECT DISTINCT date FROM spy_prices
),

-- Join all price data
combined_prices AS (
    SELECT
        d.date,
        g.gold_price,
        c.copper_price,
        o.oil_price,
        s.spy_price
    FROM all_dates AS d
    LEFT JOIN gold_prices AS g ON d.date = g.date
    LEFT JOIN copper_prices AS c ON d.date = c.date
    LEFT JOIN oil_prices AS o ON d.date = o.date
    LEFT JOIN spy_prices AS s ON d.date = s.date
    WHERE g.gold_price IS NOT NULL OR s.spy_price IS NOT NULL
),

-- Calculate ratios and moving averages
with_calculations AS (
    SELECT
        date,
        gold_price,
        copper_price,
        oil_price,
        spy_price,

        -- Copper/Gold ratio (multiply by 1000 for readability - lbs copper per oz gold)
        CASE
            WHEN gold_price > 0 AND copper_price IS NOT NULL
                THEN (copper_price / gold_price) * 1000
        END AS copper_gold_ratio,

        -- Gold/SPY ratio (oz gold per SPY share)
        CASE
            WHEN spy_price > 0 AND gold_price IS NOT NULL
                THEN gold_price / spy_price
        END AS gold_spy_ratio,

        -- Oil moving averages for momentum
        AVG(oil_price) OVER (
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS oil_sma_20,

        AVG(oil_price) OVER (
            ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS oil_sma_50,

        -- Copper/Gold moving average for trend
        AVG(CASE WHEN gold_price > 0 AND copper_price IS NOT NULL THEN (copper_price / gold_price) * 1000 END) OVER (
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS copper_gold_sma_20,

        -- Gold/SPY moving average for trend
        AVG(CASE WHEN spy_price > 0 AND gold_price IS NOT NULL THEN gold_price / spy_price END) OVER (
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS gold_spy_sma_20

    FROM combined_prices
)

SELECT
    date,
    gold_price,
    copper_price,
    oil_price,
    spy_price,

    -- Ratios
    ROUND(copper_gold_ratio, 4) AS copper_gold_ratio,
    ROUND(gold_spy_ratio, 4) AS gold_spy_ratio,

    -- Moving averages
    ROUND(oil_sma_20, 2) AS oil_sma_20,
    ROUND(oil_sma_50, 2) AS oil_sma_50,
    ROUND(copper_gold_sma_20, 4) AS copper_gold_sma_20,
    ROUND(gold_spy_sma_20, 4) AS gold_spy_sma_20,

    -- Momentum indicators
    CASE
        WHEN oil_price IS NOT NULL AND oil_sma_20 > 0
            THEN ROUND(((oil_price - oil_sma_20) / oil_sma_20) * 100, 2)
    END AS oil_momentum_pct,

    CASE
        WHEN copper_gold_ratio IS NOT NULL AND copper_gold_sma_20 > 0
            THEN ROUND(((copper_gold_ratio - copper_gold_sma_20) / copper_gold_sma_20) * 100, 2)
    END AS copper_gold_momentum_pct,

    -- Trend signals (1 = bullish, -1 = bearish, 0 = neutral)
    CASE
        WHEN oil_price > oil_sma_20 AND oil_sma_20 > oil_sma_50 THEN 1
        WHEN oil_price < oil_sma_20 AND oil_sma_20 < oil_sma_50 THEN -1
        ELSE 0
    END AS oil_trend_signal

FROM with_calculations
WHERE date >= CURRENT_DATE - INTERVAL '2 years'
ORDER BY date DESC
