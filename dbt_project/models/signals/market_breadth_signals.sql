/*
    Market Breadth Signals Model

    Calculates breadth indicators for S&P 500 stocks:
    - % of stocks above 200-day moving average (long-term trend)
    - % of stocks above 50-day moving average (short-term trend)
    - Daily advance/decline metrics
    - McClellan Oscillator & Summation Index (ratio-adjusted net advances)
    - Zweig Breadth Thrust (10d EMA of advance ratio)
    - Sector participation count (11 sector ETFs above 200d SMA)
    - Market internals correlation/dispersion (sector ETFs + SPY/QQQ)

    Used by market signals API for breadth + internals indicators.
*/

WITH RECURSIVE stock_prices AS (
    SELECT
        symbol,
        date,
        adj_close AS price,
        volume
    FROM {{ ref('stg_sp500_companies_prices') }}
    WHERE
        adj_close IS NOT NULL
        AND adj_close > 0
        AND date >= CURRENT_DATE - INTERVAL 3 YEAR
),

-- Calculate moving averages for each stock
stock_with_ma AS (
    SELECT
        symbol,
        date,
        price,
        volume,
        -- 50-day simple moving average
        AVG(price) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS sma_50,
        -- 200-day simple moving average
        AVG(price) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS sma_200,
        -- Previous day's price for advance/decline
        LAG(price, 1) OVER (
            PARTITION BY symbol
            ORDER BY date
        ) AS prev_price,
        -- Count of days in MA calculation (to ensure enough data)
        COUNT(*) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS ma_200_days_count
    FROM stock_prices
),

-- Flag stocks above/below MAs and advancing/declining
stock_signals AS (
    SELECT
        symbol,
        date,
        price,
        volume,
        sma_50,
        sma_200,
        prev_price,
        -- Only use MA if we have enough data points
        CASE WHEN ma_200_days_count >= 200 THEN sma_200 END AS valid_sma_200,
        CASE WHEN ma_200_days_count >= 50 THEN sma_50 END AS valid_sma_50,
        -- Above/below MA flags
        CASE WHEN ma_200_days_count >= 200 AND price > sma_200 THEN 1 ELSE 0 END AS above_200_ma,
        CASE WHEN ma_200_days_count >= 50 AND price > sma_50 THEN 1 ELSE 0 END AS above_50_ma,
        -- Advance/decline flags
        CASE WHEN prev_price IS NOT NULL AND price > prev_price THEN 1 ELSE 0 END AS is_advancing,
        CASE WHEN prev_price IS NOT NULL AND price < prev_price THEN 1 ELSE 0 END AS is_declining,
        CASE WHEN prev_price IS NOT NULL AND price = prev_price THEN 1 ELSE 0 END AS is_unchanged,
        -- Volume for advancing/declining
        CASE WHEN prev_price IS NOT NULL AND price > prev_price THEN volume ELSE 0 END AS advancing_volume,
        CASE WHEN prev_price IS NOT NULL AND price < prev_price THEN volume ELSE 0 END AS declining_volume
    FROM stock_with_ma
),

-- Aggregate daily breadth metrics
daily_breadth AS (
    SELECT
        date,
        -- Count metrics
        COUNT(DISTINCT symbol) AS total_stocks,
        SUM(above_200_ma) AS stocks_above_200_ma,
        SUM(above_50_ma) AS stocks_above_50_ma,
        SUM(is_advancing) AS advancing_stocks,
        SUM(is_declining) AS declining_stocks,
        SUM(is_unchanged) AS unchanged_stocks,
        -- Volume metrics
        SUM(advancing_volume) AS total_advancing_volume,
        SUM(declining_volume) AS total_declining_volume,
        -- Percentage metrics
        ROUND(100.0 * SUM(above_200_ma) / NULLIF(COUNT(DISTINCT symbol), 0), 2) AS pct_above_200_ma,
        ROUND(100.0 * SUM(above_50_ma) / NULLIF(COUNT(DISTINCT symbol), 0), 2) AS pct_above_50_ma,
        -- Advance/Decline ratio
        ROUND(1.0 * SUM(is_advancing) / NULLIF(SUM(is_declining), 0), 3) AS ad_ratio,
        -- Advance/Decline line contribution (advances - declines)
        SUM(is_advancing) - SUM(is_declining) AS ad_line_delta
    FROM stock_signals
    WHERE date >= CURRENT_DATE - INTERVAL 2 YEAR
    GROUP BY date
    HAVING COUNT(DISTINCT symbol) >= 400  -- Ensure we have most S&P 500 stocks
),

breadth_base AS (
    SELECT
        *,
        (advancing_stocks - declining_stocks) AS net_advances,
        CASE
            WHEN advancing_stocks + declining_stocks > 0 THEN ROUND(
                (advancing_stocks - declining_stocks) * 1000.0
                / (advancing_stocks + declining_stocks),
                2
            )
            ELSE 0
        END AS rana,
        CASE
            WHEN advancing_stocks + declining_stocks > 0 THEN ROUND(
                1.0 * advancing_stocks / (advancing_stocks + declining_stocks),
                6
            )
            ELSE 0.5
        END AS adv_ratio
    FROM daily_breadth
),

-- Add cumulative A/D line and moving averages
breadth_with_cum AS (
    SELECT
        *,
        -- Cumulative A/D line
        SUM(ad_line_delta) OVER (ORDER BY date) AS ad_line_cumulative,
        -- 10-day moving average of % above 200 MA
        AVG(pct_above_200_ma) OVER (
            ORDER BY date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS pct_above_200_ma_10d_avg,
        -- 5-day moving average of A/D ratio
        AVG(ad_ratio) OVER (
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS ad_ratio_5d_avg,
        -- Previous day values for momentum
        LAG(pct_above_200_ma, 1) OVER (ORDER BY date) AS prev_pct_above_200_ma,
        LAG(pct_above_50_ma, 1) OVER (ORDER BY date) AS prev_pct_above_50_ma,
        -- 5-day and 20-day change in breadth
        pct_above_200_ma - LAG(pct_above_200_ma, 5) OVER (ORDER BY date) AS breadth_5d_change,
        pct_above_200_ma - LAG(pct_above_200_ma, 20) OVER (ORDER BY date) AS breadth_20d_change,
        -- % advancing for thrust calculations
        ROUND(100.0 * advancing_stocks / NULLIF(advancing_stocks + declining_stocks, 0), 2) AS pct_advancing
    FROM breadth_base
),

-- Prepare series for EMA calculations
ema_inputs AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY date) AS rn,
        date,
        rana,
        adv_ratio
    FROM breadth_with_cum
),

-- Recursive EMA calculations for McClellan Oscillator and Zweig Thrust
ema_calc AS (
    SELECT
        rn,
        date,
        rana,
        adv_ratio,
        rana AS ema_rana_19,
        rana AS ema_rana_39,
        adv_ratio AS ema_adv_10
    FROM ema_inputs
    WHERE rn = 1

    UNION ALL

    SELECT
        i.rn,
        i.date,
        i.rana,
        i.adv_ratio,
        (0.1 * i.rana) + (0.9 * e.ema_rana_19) AS ema_rana_19,
        (0.05 * i.rana) + (0.95 * e.ema_rana_39) AS ema_rana_39,
        (0.1818181818 * i.adv_ratio) + (0.8181818182 * e.ema_adv_10) AS ema_adv_10
    FROM ema_inputs i
    INNER JOIN ema_calc e ON i.rn = e.rn + 1
),

breadth_with_ema AS (
    SELECT
        b.*,
        e.ema_rana_19,
        e.ema_rana_39,
        e.ema_adv_10,
        (e.ema_rana_19 - e.ema_rana_39) AS mcclellan_oscillator
    FROM breadth_with_cum b
    INNER JOIN ema_calc e ON b.date = e.date
),

breadth_with_mcclellan AS (
    SELECT
        *,
        SUM(mcclellan_oscillator) OVER (ORDER BY date) + 1000 AS mcclellan_summation_index,
        CASE
            WHEN ema_adv_10 >= 0.615
                AND MIN(ema_adv_10) OVER (ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) < 0.40
            THEN 1
            ELSE 0
        END AS zweig_thrust_signal
    FROM breadth_with_ema
),

-- SPY new highs for divergence checks
spy_prices AS (
    SELECT
        date,
        adj_close AS spy_close
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'SPY'
      AND adj_close IS NOT NULL
      AND date >= CURRENT_DATE - INTERVAL 3 YEAR
),

spy_with_highs AS (
    SELECT
        date,
        spy_close,
        MAX(spy_close) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS spy_high_252d
    FROM spy_prices
),

-- Sector participation (11 sector ETFs above 200d SMA)
sector_prices AS (
    SELECT
        symbol,
        date,
        adj_close AS price
    FROM {{ ref('stg_us_sectors') }}
    WHERE adj_close IS NOT NULL
      AND date >= CURRENT_DATE - INTERVAL 3 YEAR
),

sector_with_sma AS (
    SELECT
        symbol,
        date,
        price,
        AVG(price) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS sma_200,
        COUNT(*) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS sma_days
    FROM sector_prices
),

sector_participation AS (
    SELECT
        date,
        SUM(CASE WHEN sma_days >= 200 AND price > sma_200 THEN 1 ELSE 0 END) AS sector_participation_count,
        COUNT(DISTINCT symbol) AS sector_total
    FROM sector_with_sma
    GROUP BY date
),

-- Market internals (sector ETFs + SPY/QQQ)
internals_prices AS (
    SELECT
        symbol,
        date,
        adj_close AS price
    FROM {{ ref('stg_us_sectors') }}
    WHERE adj_close IS NOT NULL
      AND date >= CURRENT_DATE - INTERVAL 3 YEAR

    UNION ALL

    SELECT
        symbol,
        date,
        adj_close AS price
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol IN ('SPY', 'QQQ')
      AND adj_close IS NOT NULL
      AND date >= CURRENT_DATE - INTERVAL 3 YEAR
),

internals_returns AS (
    SELECT
        symbol,
        date,
        price,
        (price / LAG(price) OVER (PARTITION BY symbol ORDER BY date) - 1.0) AS daily_return
    FROM internals_prices
),

internals_dispersion AS (
    SELECT
        date,
        STDDEV_SAMP(daily_return) AS return_dispersion,
        AVG(ABS(daily_return)) AS avg_abs_return
    FROM internals_returns
    WHERE daily_return IS NOT NULL
    GROUP BY date
),

internals_symbols AS (
    SELECT DISTINCT
        symbol
    FROM internals_returns
),

pair_symbols AS (
    SELECT
        a.symbol AS symbol_a,
        b.symbol AS symbol_b
    FROM internals_symbols a
    INNER JOIN internals_symbols b
        ON a.symbol < b.symbol
),

pair_returns AS (
    SELECT
        r1.date,
        p.symbol_a,
        p.symbol_b,
        r1.daily_return AS return_a,
        r2.daily_return AS return_b
    FROM pair_symbols p
    INNER JOIN internals_returns r1
        ON p.symbol_a = r1.symbol
    INNER JOIN internals_returns r2
        ON p.symbol_b = r2.symbol
        AND r1.date = r2.date
    WHERE r1.daily_return IS NOT NULL
      AND r2.daily_return IS NOT NULL
),

pair_corr AS (
    SELECT
        date,
        symbol_a,
        symbol_b,
        CORR(return_a, return_b) OVER (
            PARTITION BY symbol_a, symbol_b
            ORDER BY date
            ROWS BETWEEN 62 PRECEDING AND CURRENT ROW
        ) AS pair_corr_63d
    FROM pair_returns
),

internals_correlation AS (
    SELECT
        date,
        AVG(pair_corr_63d) AS avg_pair_correlation_63d
    FROM pair_corr
    GROUP BY date
),

internals_dispersion_smoothed AS (
    SELECT
        date,
        return_dispersion,
        AVG(return_dispersion) OVER (
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS return_dispersion_20d_avg
    FROM internals_dispersion
)

SELECT
    b.date,
    b.total_stocks,
    b.stocks_above_200_ma,
    b.stocks_above_50_ma,
    b.advancing_stocks,
    b.declining_stocks,
    b.unchanged_stocks,
    b.pct_above_200_ma,
    b.pct_above_50_ma,
    b.ad_ratio,
    b.ad_line_delta,
    b.ad_line_cumulative,
    b.prev_pct_above_200_ma,
    b.prev_pct_above_50_ma,
    b.pct_advancing,
    b.total_advancing_volume,
    b.total_declining_volume,
    ROUND(b.pct_above_200_ma_10d_avg, 2) AS pct_above_200_ma_10d_avg,
    ROUND(b.ad_ratio_5d_avg, 3) AS ad_ratio_5d_avg,
    ROUND(b.breadth_5d_change, 2) AS breadth_5d_change,
    ROUND(b.breadth_20d_change, 2) AS breadth_20d_change,
    -- Volume A/D ratio
    ROUND(1.0 * b.total_advancing_volume / NULLIF(b.total_declining_volume, 0), 3) AS volume_ad_ratio,
    -- McClellan + Zweig
    b.net_advances,
    b.rana AS ratio_adjusted_net_advances,
    ROUND(b.ema_rana_19, 2) AS rana_ema_19,
    ROUND(b.ema_rana_39, 2) AS rana_ema_39,
    ROUND(b.mcclellan_oscillator, 2) AS mcclellan_oscillator,
    ROUND(b.mcclellan_summation_index, 2) AS mcclellan_summation_index,
    ROUND(b.ema_adv_10, 4) AS zweig_ema_10d,
    b.zweig_thrust_signal,
    -- SPY new highs + divergence
    s.spy_close,
    s.spy_high_252d,
    CASE WHEN s.spy_close >= s.spy_high_252d THEN 1 ELSE 0 END AS spy_new_high,
    CASE
        WHEN s.spy_close >= s.spy_high_252d
            AND b.breadth_20d_change IS NOT NULL
            AND b.breadth_20d_change < -5
        THEN 1
        ELSE 0
    END AS breadth_divergence_signal,
    -- Sector participation
    sp.sector_participation_count,
    sp.sector_total,
    ROUND(100.0 * sp.sector_participation_count / NULLIF(sp.sector_total, 0), 2) AS sector_participation_pct,
    -- Internals correlation/dispersion
    ROUND(ic.avg_pair_correlation_63d, 4) AS avg_pair_correlation_63d,
    ROUND(id.return_dispersion, 4) AS return_dispersion,
    ROUND(id.return_dispersion_20d_avg, 4) AS return_dispersion_20d_avg
FROM breadth_with_mcclellan b
LEFT JOIN spy_with_highs s ON b.date = s.date
LEFT JOIN sector_participation sp ON b.date = sp.date
LEFT JOIN internals_correlation ic ON b.date = ic.date
LEFT JOIN internals_dispersion_smoothed id ON b.date = id.date
ORDER BY b.date DESC
