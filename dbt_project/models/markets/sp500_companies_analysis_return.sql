WITH daily_data AS (
    SELECT
        symbol,
        exchange,
        date,
        close,
        high,
        low,
        volume,
        split_adj_close,
        split_adj_high,
        split_adj_low,
        split_adj_volume,
        LAG(split_adj_close, 1) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
        ) AS prev_adj_close
    FROM {{ ref('split_adjusted_prices') }}
    WHERE source_table = 'sp500_companies_prices_raw'
      AND split_adj_close IS NOT NULL
),

daily_changes AS (
    SELECT
        symbol,
        exchange,
        date,
        close,
        high,
        low,
        volume,
        split_adj_close,
        split_adj_high,
        split_adj_low,
        split_adj_volume,
        prev_adj_close,
        split_adj_close - prev_adj_close AS daily_diff
    FROM daily_data
),

price_lookups AS (
    SELECT
        dc.symbol,
        dc.exchange,
        dc.date,
        dc.split_adj_close AS current_price,
        dc.split_adj_high AS current_high,
        dc.split_adj_low AS current_low,
        dc.volume AS current_volume,
        dc.daily_diff,
        p1.split_adj_close AS price_30d_ago,
        p2.split_adj_close AS price_90d_ago,
        p3.split_adj_close AS price_180d_ago,
        p4.split_adj_close AS price_270d_ago,
        p5.split_adj_close AS price_365d_ago
    FROM daily_changes dc
    LEFT JOIN daily_changes p1
        ON dc.symbol = p1.symbol
        AND dc.exchange = p1.exchange
        AND p1.date = dc.date - INTERVAL 30 DAY
    LEFT JOIN daily_changes p2
        ON dc.symbol = p2.symbol
        AND dc.exchange = p2.exchange
        AND p2.date = dc.date - INTERVAL 90 DAY
    LEFT JOIN daily_changes p3
        ON dc.symbol = p3.symbol
        AND dc.exchange = p3.exchange
        AND p3.date = dc.date - INTERVAL 180 DAY
    LEFT JOIN daily_changes p4
        ON dc.symbol = p4.symbol
        AND dc.exchange = p4.exchange
        AND p4.date = dc.date - INTERVAL 270 DAY
    LEFT JOIN daily_changes p5
        ON dc.symbol = p5.symbol
        AND dc.exchange = p5.exchange
        AND p5.date = dc.date - INTERVAL 365 DAY
),

rolling_stats AS (
    SELECT
        symbol,
        exchange,
        date,
        current_price,
        current_high,
        current_low,
        current_volume,

        MAX(current_high) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 365 DAY PRECEDING AND CURRENT ROW
        ) AS high_1yr,
        MIN(current_low) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 365 DAY PRECEDING AND CURRENT ROW
        ) AS low_1yr,
        STDDEV(daily_diff) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 365 DAY PRECEDING AND CURRENT ROW
        ) AS std_diff_1yr,
        price_365d_ago AS price_start_1yr,
        CASE
            WHEN price_365d_ago IS NOT NULL AND price_365d_ago > 0
            THEN (current_price - price_365d_ago) / price_365d_ago * 100
            ELSE NULL
        END AS pct_change_1yr,

        MAX(current_high) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 270 DAY PRECEDING AND CURRENT ROW
        ) AS high_9mo,
        MIN(current_low) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 270 DAY PRECEDING AND CURRENT ROW
        ) AS low_9mo,
        STDDEV(daily_diff) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 270 DAY PRECEDING AND CURRENT ROW
        ) AS std_diff_9mo,
        price_270d_ago AS price_start_9mo,
        CASE
            WHEN price_270d_ago IS NOT NULL AND price_270d_ago > 0
            THEN (current_price - price_270d_ago) / price_270d_ago * 100
            ELSE NULL
        END AS pct_change_9mo,

        MAX(current_high) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 180 DAY PRECEDING AND CURRENT ROW
        ) AS high_6mo,
        MIN(current_low) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 180 DAY PRECEDING AND CURRENT ROW
        ) AS low_6mo,
        STDDEV(daily_diff) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 180 DAY PRECEDING AND CURRENT ROW
        ) AS std_diff_6mo,
        price_180d_ago AS price_start_6mo,
        CASE
            WHEN price_180d_ago IS NOT NULL AND price_180d_ago > 0
            THEN (current_price - price_180d_ago) / price_180d_ago * 100
            ELSE NULL
        END AS pct_change_6mo,

        MAX(current_high) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 90 DAY PRECEDING AND CURRENT ROW
        ) AS high_3mo,
        MIN(current_low) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 90 DAY PRECEDING AND CURRENT ROW
        ) AS low_3mo,
        STDDEV(daily_diff) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 90 DAY PRECEDING AND CURRENT ROW
        ) AS std_diff_3mo,
        price_90d_ago AS price_start_3mo,
        CASE
            WHEN price_90d_ago IS NOT NULL AND price_90d_ago > 0
            THEN (current_price - price_90d_ago) / price_90d_ago * 100
            ELSE NULL
        END AS pct_change_3mo,

        MAX(current_high) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
        ) AS high_1mo,
        MIN(current_low) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
        ) AS low_1mo,
        STDDEV(daily_diff) OVER (
            PARTITION BY symbol, exchange
            ORDER BY date
            RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
        ) AS std_diff_1mo,
        price_30d_ago AS price_start_1mo,
        CASE
            WHEN price_30d_ago IS NOT NULL AND price_30d_ago > 0
            THEN (current_price - price_30d_ago) / price_30d_ago * 100
            ELSE NULL
        END AS pct_change_1mo
    FROM price_lookups
)

SELECT * FROM rolling_stats
