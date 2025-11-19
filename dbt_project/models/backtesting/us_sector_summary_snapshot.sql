{{ config(
    materialized='incremental',
    unique_key=['snapshot_date', 'symbol', 'asset_type', 'time_period'],
    incremental_strategy='delete+insert'
) }}

WITH snapshot_dates AS (
    -- Generate snapshot dates (first day of each month) from available data
    SELECT DISTINCT
        DATE_TRUNC('month', date) AS snapshot_date
    FROM {{ ref('stg_us_sectors') }}
    WHERE date >= '2020-01-01'  -- Adjust based on your data availability
),

base_data AS (
    SELECT
        sd.snapshot_date,
        symbol,
        asset_type,
        open,
        close,
        adj_open,
        adj_close,
        exchange,
        name,
        CAST(date AS DATE) AS trade_date,
        -- Calculate price changes
        (close - open) AS price_change_raw,
        (adj_close - adj_open) AS price_change_adj,
        -- Calculate percentage changes
        CASE
            WHEN open > 0 THEN ((close - open) / open) * 100
        END AS pct_change_raw,
        CASE
            WHEN adj_open > 0 THEN ((adj_close - adj_open) / adj_open) * 100
        END AS pct_change_adj
    FROM {{ ref('stg_us_sectors') }}
    CROSS JOIN snapshot_dates AS sd
    WHERE
        date IS NOT NULL
        AND open IS NOT NULL
        AND close IS NOT NULL
        AND open > 0
        AND trade_date <= sd.snapshot_date
        AND trade_date >= sd.snapshot_date - INTERVAL '5 years'
),

-- Define date boundaries for different periods relative to snapshot_date
date_boundaries AS (
    SELECT
        snapshot_date AS today,
        snapshot_date - INTERVAL '12 weeks' AS twelve_weeks_ago,
        snapshot_date - INTERVAL '6 months' AS six_months_ago,
        snapshot_date - INTERVAL '1 year' AS one_year_ago,
        snapshot_date - INTERVAL '5 years' AS five_years_ago,
        snapshot_date
    FROM snapshot_dates
),

-- Filter data for each time period
filtered_data AS (
    SELECT
        bd.*,
        CASE
            WHEN bd.trade_date >= db.twelve_weeks_ago THEN '12_weeks'
            WHEN bd.trade_date >= db.six_months_ago THEN '6_months'
            WHEN bd.trade_date >= db.one_year_ago THEN '1_year'
            WHEN bd.trade_date >= db.five_years_ago THEN '5_years'
            ELSE 'older'
        END AS time_period
    FROM base_data AS bd
    INNER JOIN date_boundaries AS db
        ON bd.snapshot_date = db.snapshot_date
    WHERE bd.trade_date >= db.five_years_ago
),

-- Get first and last prices for each period
period_boundaries AS (
    SELECT
        snapshot_date,
        symbol,
        asset_type,
        time_period,
        MIN(trade_date) AS period_start_date,
        MAX(trade_date) AS period_end_date
    FROM filtered_data
    WHERE time_period != 'older'
    GROUP BY snapshot_date, symbol, asset_type, time_period
),

-- Get start and end prices
start_prices AS (
    SELECT
        pb.snapshot_date,
        pb.symbol,
        pb.asset_type,
        pb.time_period,
        fd.adj_open AS period_start_price
    FROM period_boundaries AS pb
    INNER JOIN filtered_data AS fd ON
        pb.snapshot_date = fd.snapshot_date
        AND pb.symbol = fd.symbol
        AND pb.asset_type = fd.asset_type
        AND pb.time_period = fd.time_period
        AND pb.period_start_date = fd.trade_date
),

end_prices AS (
    SELECT
        pb.snapshot_date,
        pb.symbol,
        pb.asset_type,
        pb.time_period,
        fd.adj_close AS period_end_price
    FROM period_boundaries AS pb
    INNER JOIN filtered_data AS fd ON
        pb.snapshot_date = fd.snapshot_date
        AND pb.symbol = fd.symbol
        AND pb.asset_type = fd.asset_type
        AND pb.time_period = fd.time_period
        AND pb.period_end_date = fd.trade_date
),

-- Main aggregation
aggregated_results AS (
    SELECT
        snapshot_date,
        symbol,
        asset_type,
        time_period,
        exchange,
        name,
        MIN(trade_date) AS period_start_date,
        MAX(trade_date) AS period_end_date,
        COUNT(*) AS trading_days,
        SUM(price_change_raw) AS total_price_change_raw,
        AVG(price_change_raw) AS avg_daily_price_change_raw,
        STDDEV(price_change_raw) AS stddev_price_change_raw,
        MIN(price_change_raw) AS min_daily_change_raw,
        MAX(price_change_raw) AS max_daily_change_raw,
        SUM(price_change_adj) AS total_price_change_adj,
        AVG(price_change_adj) AS avg_daily_price_change_adj,
        STDDEV(price_change_adj) AS stddev_price_change_adj,
        MIN(price_change_adj) AS min_daily_change_adj,
        MAX(price_change_adj) AS max_daily_change_adj,
        AVG(pct_change_raw) AS avg_daily_pct_change_raw,
        STDDEV(pct_change_raw) AS stddev_pct_change_raw,
        MIN(pct_change_raw) AS min_daily_pct_change_raw,
        MAX(pct_change_raw) AS max_daily_pct_change_raw,
        AVG(pct_change_adj) AS avg_daily_pct_change_adj,
        STDDEV(pct_change_adj) AS stddev_pct_change_adj,
        MIN(pct_change_adj) AS min_daily_pct_change_adj,
        MAX(pct_change_adj) AS max_daily_pct_change_adj,
        SUM(CASE WHEN price_change_adj > 0 THEN 1 ELSE 0 END) AS positive_days,
        SUM(CASE WHEN price_change_adj < 0 THEN 1 ELSE 0 END) AS negative_days,
        SUM(CASE WHEN price_change_adj = 0 THEN 1 ELSE 0 END) AS neutral_days
    FROM filtered_data
    WHERE time_period != 'older'
    GROUP BY snapshot_date, symbol, asset_type, time_period, exchange, name
),

-- Combine aggregated results with period boundary prices
combined_results AS (
    SELECT
        ar.*,
        sp.period_start_price,
        ep.period_end_price
    FROM aggregated_results AS ar
    LEFT JOIN start_prices AS sp
        ON ar.snapshot_date = sp.snapshot_date
        AND ar.symbol = sp.symbol
        AND ar.asset_type = sp.asset_type
        AND ar.time_period = sp.time_period
    LEFT JOIN end_prices AS ep
        ON ar.snapshot_date = ep.snapshot_date
        AND ar.symbol = ep.symbol
        AND ar.asset_type = ep.asset_type
        AND ar.time_period = ep.time_period
),

-- Calculate final metrics
final_metrics AS (
    SELECT
        *,
        CASE
            WHEN period_start_price > 0
                THEN ((period_end_price - period_start_price) / period_start_price) * 100
        END AS total_period_return_pct,
        CASE
            WHEN trading_days > 0 THEN (positive_days * 100.0) / trading_days
        END AS win_rate_pct,
        stddev_pct_change_adj * SQRT(252) AS annualized_volatility_pct
    FROM combined_results
)

-- Final results
SELECT
    snapshot_date,
    symbol,
    asset_type,
    time_period,
    exchange,
    name,
    period_start_date,
    period_end_date,
    trading_days,
    positive_days,
    negative_days,
    neutral_days,
    ROUND(total_period_return_pct, 2) AS total_return_pct,
    ROUND(avg_daily_pct_change_adj, 4) AS avg_daily_return_pct,
    ROUND(annualized_volatility_pct, 2) AS volatility_pct,
    ROUND(win_rate_pct, 1) AS win_rate_pct,
    ROUND(total_price_change_adj, 2) AS total_price_change,
    ROUND(avg_daily_price_change_adj, 4) AS avg_daily_price_change,
    ROUND(min_daily_change_adj, 2) AS worst_day_change,
    ROUND(max_daily_change_adj, 2) AS best_day_change,
    ROUND(period_start_price, 2) AS period_start_price,
    ROUND(period_end_price, 2) AS period_end_price
FROM final_metrics
ORDER BY snapshot_date DESC, time_period, asset_type, symbol

