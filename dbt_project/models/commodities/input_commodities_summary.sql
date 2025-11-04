{{ config(
    materialized='view'
) }}

WITH base_data AS (
    SELECT
        commodity_name,
        commodity_unit,
        price,
        CAST(date AS DATE) AS trade_date,
        -- Calculate day-over-day price changes
        price - LAG(price) OVER (
            PARTITION BY commodity_name
            ORDER BY date
        ) AS price_change,
        -- Calculate percentage changes
        CASE
            WHEN LAG(price) OVER (
                PARTITION BY commodity_name
                ORDER BY date
            ) > 0
                THEN (
                    (price - LAG(price) OVER (
                        PARTITION BY commodity_name
                        ORDER BY date
                    ))
                    / LAG(price) OVER (
                        PARTITION BY commodity_name
                        ORDER BY date
                    )
                )
                * 100
        END AS pct_change
    FROM {{ ref('stg_input_commodities') }}
    WHERE price IS NOT NULL
      AND date IS NOT NULL
      AND price > 0
),

-- Define date boundaries for different periods
date_boundaries AS (
    SELECT
        CURRENT_DATE AS today,
        CURRENT_DATE - INTERVAL '12 weeks' AS twelve_weeks_ago,
        CURRENT_DATE - INTERVAL '6 months' AS six_months_ago,
        CURRENT_DATE - INTERVAL '1 year' AS one_year_ago,
        CURRENT_DATE - INTERVAL '5 years' AS five_years_ago
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
    CROSS JOIN date_boundaries AS db
    WHERE bd.trade_date >= db.five_years_ago
      AND bd.price_change IS NOT NULL
),

-- Get first and last prices for each period
period_boundaries AS (
    SELECT
        commodity_name,
        commodity_unit,
        time_period,
        MIN(trade_date) AS period_start_date,
        MAX(trade_date) AS period_end_date
    FROM filtered_data
    WHERE time_period != 'older'
    GROUP BY commodity_name, commodity_unit, time_period
),

-- Get start and end prices
start_prices AS (
    SELECT
        pb.commodity_name,
        pb.commodity_unit,
        pb.time_period,
        fd.price AS period_start_price
    FROM period_boundaries AS pb
    INNER JOIN filtered_data AS fd ON
        pb.commodity_name = fd.commodity_name
        AND pb.time_period = fd.time_period
        AND pb.period_start_date = fd.trade_date
),

end_prices AS (
    SELECT
        pb.commodity_name,
        pb.commodity_unit,
        pb.time_period,
        fd.price AS period_end_price
    FROM period_boundaries AS pb
    INNER JOIN filtered_data AS fd ON
        pb.commodity_name = fd.commodity_name
        AND pb.time_period = fd.time_period
        AND pb.period_end_date = fd.trade_date
),

-- Main aggregation
aggregated_results AS (
    SELECT
        commodity_name,
        commodity_unit,
        time_period,
        MIN(trade_date) AS period_start_date,
        MAX(trade_date) AS period_end_date,
        COUNT(*) AS trading_days,
        SUM(price_change) AS total_price_change,
        AVG(price_change) AS avg_daily_price_change,
        STDDEV(price_change) AS stddev_price_change,
        MIN(price_change) AS min_daily_change,
        MAX(price_change) AS max_daily_change,
        AVG(pct_change) AS avg_daily_pct_change,
        STDDEV(pct_change) AS stddev_pct_change,
        MIN(pct_change) AS min_daily_pct_change,
        MAX(pct_change) AS max_daily_pct_change,
        SUM(CASE WHEN price_change > 0 THEN 1 ELSE 0 END) AS positive_days,
        SUM(CASE WHEN price_change < 0 THEN 1 ELSE 0 END) AS negative_days,
        SUM(CASE WHEN price_change = 0 THEN 1 ELSE 0 END) AS neutral_days
    FROM filtered_data
    WHERE time_period != 'older'
    GROUP BY commodity_name, commodity_unit, time_period
),

-- Combine aggregated results with period boundary prices
combined_results AS (
    SELECT
        ar.*,
        sp.period_start_price,
        ep.period_end_price
    FROM aggregated_results AS ar
    LEFT JOIN start_prices AS sp
        ON ar.commodity_name = sp.commodity_name
        AND ar.time_period = sp.time_period
    LEFT JOIN end_prices AS ep
        ON ar.commodity_name = ep.commodity_name
        AND ar.time_period = ep.time_period
),

-- Calculate final metrics
final_metrics AS (
    SELECT
        *,
        CASE
            WHEN period_start_price > 0
                THEN (
                    (period_end_price - period_start_price)
                    / period_start_price
                )
                * 100
        END AS total_period_return_pct,
        CASE
            WHEN trading_days > 0
                THEN (positive_days * 100.0) / trading_days
        END AS win_rate_pct,
        stddev_pct_change * SQRT(252) AS annualized_volatility_pct
    FROM combined_results
)

-- Final results
SELECT
    commodity_name,
    commodity_unit,
    time_period,
    period_start_date,
    period_end_date,
    trading_days,
    positive_days,
    negative_days,
    neutral_days,
    ROUND(total_period_return_pct, 2) AS total_return_pct,
    ROUND(avg_daily_pct_change, 4) AS avg_daily_return_pct,
    ROUND(annualized_volatility_pct, 2) AS volatility_pct,
    ROUND(win_rate_pct, 1) AS win_rate_pct,
    ROUND(total_price_change, 2) AS total_price_change,
    ROUND(avg_daily_price_change, 4) AS avg_daily_price_change,
    ROUND(min_daily_change, 2) AS worst_day_change,
    ROUND(max_daily_change, 2) AS best_day_change,
    ROUND(period_start_price, 2) AS period_start_price,
    ROUND(period_end_price, 2) AS period_end_price
FROM final_metrics
ORDER BY time_period, commodity_name

