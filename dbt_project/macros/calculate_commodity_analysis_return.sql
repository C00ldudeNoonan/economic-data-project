{% macro calculate_commodity_analysis_return(source_ref) %}
WITH daily_data AS (
    SELECT
        commodity_name,
        commodity_unit,
        date,
        price,
        LAG(price, 1) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY date
        ) AS prev_price
    FROM {{ ref(source_ref) }}
    WHERE price IS NOT NULL
),

daily_changes AS (
    SELECT
        commodity_name,
        commodity_unit,
        date,
        price,
        prev_price,
        price - prev_price AS daily_diff
    FROM daily_data
),

price_lookups AS (
    SELECT
        dc.commodity_name,
        dc.commodity_unit,
        dc.date,
        dc.price AS current_price,
        dc.daily_diff,
        p1.price AS price_30d_ago,
        p2.price AS price_90d_ago,
        p3.price AS price_180d_ago,
        p4.price AS price_270d_ago,
        p5.price AS price_365d_ago
    FROM daily_changes dc
    LEFT JOIN daily_changes p1
        ON dc.commodity_name = p1.commodity_name
        AND dc.commodity_unit = p1.commodity_unit
        AND p1.date = dc.date - INTERVAL 30 DAY
    LEFT JOIN daily_changes p2
        ON dc.commodity_name = p2.commodity_name
        AND dc.commodity_unit = p2.commodity_unit
        AND p2.date = dc.date - INTERVAL 90 DAY
    LEFT JOIN daily_changes p3
        ON dc.commodity_name = p3.commodity_name
        AND dc.commodity_unit = p3.commodity_unit
        AND p3.date = dc.date - INTERVAL 180 DAY
    LEFT JOIN daily_changes p4
        ON dc.commodity_name = p4.commodity_name
        AND dc.commodity_unit = p4.commodity_unit
        AND p4.date = dc.date - INTERVAL 270 DAY
    LEFT JOIN daily_changes p5
        ON dc.commodity_name = p5.commodity_name
        AND dc.commodity_unit = p5.commodity_unit
        AND p5.date = dc.date - INTERVAL 365 DAY
),

rolling_stats AS (
    SELECT
        commodity_name,
        commodity_unit,
        date,
        current_price,

        MAX(current_price) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 365 PRECEDING AND CURRENT ROW
        ) AS high_1yr,
        MIN(current_price) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 365 PRECEDING AND CURRENT ROW
        ) AS low_1yr,
        STDDEV(daily_diff) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 365 PRECEDING AND CURRENT ROW
        ) AS std_diff_1yr,
        price_365d_ago AS price_start_1yr,
        CASE
            WHEN price_365d_ago IS NOT NULL AND price_365d_ago > 0
            THEN (current_price - price_365d_ago) / price_365d_ago * 100
            ELSE NULL
        END AS pct_change_1yr,

        MAX(current_price) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 270 PRECEDING AND CURRENT ROW
        ) AS high_9mo,
        MIN(current_price) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 270 PRECEDING AND CURRENT ROW
        ) AS low_9mo,
        STDDEV(daily_diff) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 270 PRECEDING AND CURRENT ROW
        ) AS std_diff_9mo,
        price_270d_ago AS price_start_9mo,
        CASE
            WHEN price_270d_ago IS NOT NULL AND price_270d_ago > 0
            THEN (current_price - price_270d_ago) / price_270d_ago * 100
            ELSE NULL
        END AS pct_change_9mo,

        MAX(current_price) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 180 PRECEDING AND CURRENT ROW
        ) AS high_6mo,
        MIN(current_price) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 180 PRECEDING AND CURRENT ROW
        ) AS low_6mo,
        STDDEV(daily_diff) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 180 PRECEDING AND CURRENT ROW
        ) AS std_diff_6mo,
        price_180d_ago AS price_start_6mo,
        CASE
            WHEN price_180d_ago IS NOT NULL AND price_180d_ago > 0
            THEN (current_price - price_180d_ago) / price_180d_ago * 100
            ELSE NULL
        END AS pct_change_6mo,

        MAX(current_price) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 90 PRECEDING AND CURRENT ROW
        ) AS high_3mo,
        MIN(current_price) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 90 PRECEDING AND CURRENT ROW
        ) AS low_3mo,
        STDDEV(daily_diff) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 90 PRECEDING AND CURRENT ROW
        ) AS std_diff_3mo,
        price_90d_ago AS price_start_3mo,
        CASE
            WHEN price_90d_ago IS NOT NULL AND price_90d_ago > 0
            THEN (current_price - price_90d_ago) / price_90d_ago * 100
            ELSE NULL
        END AS pct_change_3mo,

        MAX(current_price) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS high_1mo,
        MIN(current_price) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS low_1mo,
        STDDEV(daily_diff) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY UNIX_DATE(date)
            RANGE BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS std_diff_1mo,
        price_30d_ago AS price_start_1mo,
        CASE
            WHEN price_30d_ago IS NOT NULL AND price_30d_ago > 0
            THEN (current_price - price_30d_ago) / price_30d_ago * 100
            ELSE NULL
        END AS pct_change_1mo

    FROM price_lookups
)

SELECT
    commodity_name,
    commodity_unit,
    date,
    current_price,

    ROUND(high_1yr, 4) AS high_1yr,
    ROUND(low_1yr, 4) AS low_1yr,
    ROUND(std_diff_1yr, 4) AS std_diff_1yr,
    ROUND(pct_change_1yr, 2) AS pct_change_1yr,

    ROUND(high_9mo, 4) AS high_9mo,
    ROUND(low_9mo, 4) AS low_9mo,
    ROUND(std_diff_9mo, 4) AS std_diff_9mo,
    ROUND(pct_change_9mo, 2) AS pct_change_9mo,

    ROUND(high_6mo, 4) AS high_6mo,
    ROUND(low_6mo, 4) AS low_6mo,
    ROUND(std_diff_6mo, 4) AS std_diff_6mo,
    ROUND(pct_change_6mo, 2) AS pct_change_6mo,

    ROUND(high_3mo, 4) AS high_3mo,
    ROUND(low_3mo, 4) AS low_3mo,
    ROUND(std_diff_3mo, 4) AS std_diff_3mo,
    ROUND(pct_change_3mo, 2) AS pct_change_3mo,

    ROUND(high_1mo, 4) AS high_1mo,
    ROUND(low_1mo, 4) AS low_1mo,
    ROUND(std_diff_1mo, 4) AS std_diff_1mo,
    ROUND(pct_change_1mo, 2) AS pct_change_1mo

FROM rolling_stats
ORDER BY commodity_name, commodity_unit, date
{% endmacro %}
