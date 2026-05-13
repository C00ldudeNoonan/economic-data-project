{{ config(
    description='Technical signals: RSI, Bollinger Bands, Z-Score, VIX Mean Reversion'
) }}

/*
    Technical Signals Model

    Calculates technical and mean-reversion signals from daily price data:
    - RSI(14) and RSI(2): Relative Strength Index at two timeframes
    - Bollinger Bands: 20-day bands, bandwidth, squeeze detection
    - Z-Score: 60-day rolling z-score of SPY price
    - VIX Mean Reversion: VIX 252-day percentile rank

    Source tables: stg_major_indices (SPY daily OHLC), stg_fred_series (VIXCLS)
*/

WITH spy_daily AS (
    SELECT
        date,
        adj_close,
        adj_close - LAG(adj_close) OVER (ORDER BY date) AS daily_change
    FROM {{ ref('stg_major_indices') }}
    WHERE symbol = 'SPY'
      AND adj_close IS NOT NULL
),

daily_gains_losses AS (
    SELECT
        date,
        adj_close,
        daily_change,
        CASE WHEN daily_change > 0 THEN daily_change ELSE 0 END AS gain,
        CASE WHEN daily_change < 0 THEN ABS(daily_change) ELSE 0 END AS loss
    FROM spy_daily
    WHERE daily_change IS NOT NULL
),

rsi_components AS (
    SELECT
        date,
        adj_close,
        daily_change,
        -- 14-day average gain/loss for RSI(14)
        AVG(gain) OVER (ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
        AVG(loss) OVER (ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14,
        -- 2-day average gain/loss for RSI(2)
        AVG(gain) OVER (ORDER BY date ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_gain_2,
        AVG(loss) OVER (ORDER BY date ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS avg_loss_2
    FROM daily_gains_losses
),

rsi_calc AS (
    SELECT
        date,
        adj_close,
        daily_change,
        ROUND(100 - 100.0 / (1 + avg_gain_14 / NULLIF(avg_loss_14, 0)), 2) AS rsi_14,
        ROUND(100 - 100.0 / (1 + avg_gain_2 / NULLIF(avg_loss_2, 0)), 2) AS rsi_2
    FROM rsi_components
),

bollinger AS (
    SELECT
        date,
        adj_close,
        -- 20-day SMA and standard deviation
        AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS bb_middle,
        STDDEV(adj_close) OVER (ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS bb_stddev
    FROM spy_daily
    WHERE adj_close IS NOT NULL
),

bollinger_calc AS (
    SELECT
        date,
        adj_close,
        ROUND(bb_middle + 2 * bb_stddev, 2) AS bb_upper,
        ROUND(bb_middle - 2 * bb_stddev, 2) AS bb_lower,
        bb_middle,
        -- Bandwidth = (upper - lower) / middle * 100
        ROUND((4 * bb_stddev) / NULLIF(bb_middle, 0) * 100, 4) AS bb_bandwidth,
        -- Position within bands: (price - lower) / (upper - lower)
        ROUND(
            (adj_close - (bb_middle - 2 * bb_stddev))
            / NULLIF(4 * bb_stddev, 0),
            4
        ) AS bb_position
    FROM bollinger
),

bollinger_with_pctile AS (
    SELECT
        *,
        -- 126-day (6mo) bandwidth percentile: position of current value within rolling range
        -- (current - min) / (max - min) gives 0.0 at minimum to 1.0 at maximum
        (bb_bandwidth - MIN(bb_bandwidth) OVER (ORDER BY date ROWS BETWEEN 125 PRECEDING AND CURRENT ROW))
        / NULLIF(
            MAX(bb_bandwidth) OVER (ORDER BY date ROWS BETWEEN 125 PRECEDING AND CURRENT ROW)
            - MIN(bb_bandwidth) OVER (ORDER BY date ROWS BETWEEN 125 PRECEDING AND CURRENT ROW),
            0
        ) AS bb_bandwidth_pctile
    FROM bollinger_calc
),

zscore AS (
    SELECT
        date,
        adj_close,
        AVG(adj_close) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS mean_60d,
        STDDEV(adj_close) OVER (ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS std_60d
    FROM spy_daily
    WHERE adj_close IS NOT NULL
),

zscore_calc AS (
    SELECT
        date,
        ROUND((adj_close - mean_60d) / NULLIF(std_60d, 0), 2) AS zscore_60d
    FROM zscore
),

vix_data AS (
    SELECT
        date,
        literal AS vix_value
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'VIXCLS'
      AND literal IS NOT NULL
),

vix_with_stats AS (
    SELECT
        date,
        vix_value,
        -- 252-day percentile rank: position of current VIX within rolling range
        (vix_value - MIN(vix_value) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW))
        / NULLIF(
            MAX(vix_value) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)
            - MIN(vix_value) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW),
            0
        ) AS vix_percentile_1yr,
        -- VIX z-score (252-day)
        ROUND(
            (vix_value - AVG(vix_value) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW))
            / NULLIF(STDDEV(vix_value) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW), 0),
            2
        ) AS vix_zscore
    FROM vix_data
),

combined AS (
    SELECT
        r.date,
        r.adj_close,
        r.rsi_14,
        r.rsi_2,
        b.bb_upper,
        b.bb_lower,
        ROUND(b.bb_bandwidth, 2) AS bb_bandwidth,
        ROUND(b.bb_bandwidth_pctile, 4) AS bb_bandwidth_pctile,
        ROUND(b.bb_position, 4) AS bb_position,
        z.zscore_60d,
        v.vix_value,
        ROUND(v.vix_percentile_1yr, 4) AS vix_percentile_1yr,
        v.vix_zscore
    FROM rsi_calc AS r
    LEFT JOIN bollinger_with_pctile AS b ON r.date = b.date
    LEFT JOIN zscore_calc AS z ON r.date = z.date
    LEFT JOIN vix_with_stats AS v ON r.date = v.date
)

SELECT
    date,
    adj_close,
    rsi_14,
    rsi_2,
    bb_upper,
    bb_lower,
    bb_bandwidth,
    bb_bandwidth_pctile,
    bb_position,
    zscore_60d,
    vix_value,
    vix_percentile_1yr,
    vix_zscore,

    -- Signal: RSI
    CASE
        WHEN rsi_14 > 70 THEN 'high'
        WHEN rsi_14 < 30 THEN 'medium'
        ELSE 'normal'
    END AS rsi_status,

    -- Signal: Bollinger Squeeze
    CASE
        WHEN bb_bandwidth_pctile <= 0.10 THEN 'high'
        WHEN bb_position > 0.95 OR bb_position < 0.05 THEN 'medium'
        ELSE 'normal'
    END AS bollinger_status,

    -- Signal: Z-Score
    CASE
        WHEN ABS(zscore_60d) > 2.0 THEN 'high'
        WHEN ABS(zscore_60d) > 1.5 THEN 'medium'
        ELSE 'normal'
    END AS zscore_status,

    -- Signal: VIX Mean Reversion
    CASE
        WHEN vix_percentile_1yr > 0.90 THEN 'high'
        WHEN vix_percentile_1yr > 0.80 THEN 'medium'
        WHEN vix_percentile_1yr < 0.10 THEN 'low'
        ELSE 'normal'
    END AS vix_mean_reversion_status

FROM combined
WHERE date >= CURRENT_DATE - INTERVAL '3 years'
ORDER BY date DESC
