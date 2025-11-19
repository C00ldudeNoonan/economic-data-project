{{ config(
    materialized='incremental',
    unique_key=['snapshot_date', 'commodity_name', 'commodity_unit', 'time_period'],
    incremental_strategy='delete+insert'
) }}

WITH snapshot_dates AS (
    SELECT DISTINCT
        DATE_TRUNC('month', date) AS snapshot_date
    FROM {{ ref('stg_energy_commodities') }}
    WHERE date >= '2020-01-01'
),

base_data AS (
    SELECT
        sd.snapshot_date,
        commodity_name,
        commodity_unit,
        price,
        CAST(date AS DATE) AS trade_date,
        price - LAG(price) OVER (
            PARTITION BY sd.snapshot_date, commodity_name
            ORDER BY date
        ) AS price_change,
        CASE
            WHEN LAG(price) OVER (
                PARTITION BY sd.snapshot_date, commodity_name
                ORDER BY date
            ) > 0
            THEN (
                (price - LAG(price) OVER (
                    PARTITION BY sd.snapshot_date, commodity_name
                    ORDER BY date
                ))
                / LAG(price) OVER (
                    PARTITION BY sd.snapshot_date, commodity_name
                    ORDER BY date
                )
            ) * 100
        END AS pct_change
    FROM {{ ref('stg_energy_commodities') }}
    CROSS JOIN snapshot_dates AS sd
    WHERE
        price IS NOT NULL
        AND date IS NOT NULL
        AND price > 0
        AND trade_date <= sd.snapshot_date
        AND trade_date >= sd.snapshot_date - INTERVAL '5 years'
),

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
    WHERE
        bd.trade_date >= db.five_years_ago
        AND bd.price_change IS NOT NULL
),

period_boundaries AS (
    SELECT
        snapshot_date,
        commodity_name,
        commodity_unit,
        time_period,
        MIN(trade_date) AS period_start_date,
        MAX(trade_date) AS period_end_date
    FROM filtered_data
    WHERE time_period != 'older'
    GROUP BY snapshot_date, commodity_name, commodity_unit, time_period
),

start_prices AS (
    SELECT
        pb.snapshot_date,
        pb.commodity_name,
        pb.commodity_unit,
        pb.time_period,
        fd.price AS period_start_price
    FROM period_boundaries AS pb
    INNER JOIN filtered_data AS fd ON
        pb.snapshot_date = fd.snapshot_date
        AND pb.commodity_name = fd.commodity_name
        AND pb.time_period = fd.time_period
        AND pb.period_start_date = fd.trade_date
),

end_prices AS (
    SELECT
        pb.snapshot_date,
        pb.commodity_name,
        pb.commodity_unit,
        pb.time_period,
        fd.price AS period_end_price
    FROM period_boundaries AS pb
    INNER JOIN filtered_data AS fd ON
        pb.snapshot_date = fd.snapshot_date
        AND pb.commodity_name = fd.commodity_name
        AND pb.time_period = fd.time_period
        AND pb.period_end_date = fd.trade_date
),

aggregated_results AS (
    SELECT
        snapshot_date,
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
    GROUP BY snapshot_date, commodity_name, commodity_unit, time_period
),

combined_results AS (
    SELECT
        ar.*,
        sp.period_start_price,
        ep.period_end_price
    FROM aggregated_results AS ar
    LEFT JOIN start_prices AS sp
        ON ar.snapshot_date = sp.snapshot_date
        AND ar.commodity_name = sp.commodity_name
        AND ar.time_period = sp.time_period
    LEFT JOIN end_prices AS ep
        ON ar.snapshot_date = ep.snapshot_date
        AND ar.commodity_name = ep.commodity_name
        AND ar.time_period = ep.time_period
),

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
        stddev_pct_change * SQRT(252) AS annualized_volatility_pct
    FROM combined_results
)

SELECT
    snapshot_date,
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
ORDER BY snapshot_date DESC, time_period, commodity_name

