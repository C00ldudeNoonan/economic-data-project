{{ config(
    description='Wide daily technical-indicator feature table at symbol/date grain (issue #109, phase 1)'
) }}

/*
    Technical Indicator Daily

    Grain: one row per source_universe, symbol, exchange, date.

    Phase-1 indicator set (see issue #109 acceptance criteria):
    SMA(20/50/200), EMA(12/26), RSI(14, Wilder), MACD(12/26/9), Bollinger
    Bands(20, 2σ), ATR/NATR(14, Wilder), ADX/+DI/-DI(14), Stochastic
    %K(14)/%D(3), ROC(20), Williams %R(14), CCI(20), OBV, MFI(14),
    Donchian(20, prior-bar), relative volume(20), plus rolling z-score,
    52-week high/low distance, and daily/log returns.

    Implementation notes:
    - EMA/RMA use the project-standard finite-window exponentially
      weighted mean (see ta_ewm_from_array) rather than a recursive UDF.
    - All indicators are NULL until enough lookback bars exist (warmup
      gating on bars_available), so early rows never carry misleading
      partially-warmed values.
    - Donchian levels exclude the current bar so a breakout can be
      detected against yesterday's channel (no self-confirmation).
    - No future data is referenced anywhere in this model.
*/

{% set ewm_lookback = 60 %}

WITH base AS (
    SELECT
        source_universe,
        symbol,
        exchange,
        name,
        asset_type,
        date,
        bars_available,
        open,
        high,
        low,
        close,
        volume,
        {{ ta_typical_price() }} AS typical_price,
        {{ ta_lag('close') }} AS prev_close,
        {{ ta_lag('high') }} AS prev_high,
        {{ ta_lag('low') }} AS prev_low,
        {{ ta_lag(ta_typical_price()) }} AS prev_typical_price,
        {{ ta_lag('close', 20) }} AS close_20_ago
    FROM {{ ref('technical_price_universe') }}
),

flows AS (
    SELECT
        *,
        {{ ta_daily_return() }} AS daily_return,
        {{ ta_log_return() }} AS log_return,
        GREATEST(close - prev_close, 0) AS gain,
        GREATEST(prev_close - close, 0) AS loss,
        {{ ta_true_range() }} AS true_range,
        -- Directional movement (Wilder): only the larger of the two moves
        -- counts, and only when positive
        CASE
            WHEN high - prev_high > prev_low - low AND high - prev_high > 0
                THEN high - prev_high
            ELSE 0
        END AS plus_dm,
        CASE
            WHEN prev_low - low > high - prev_high AND prev_low - low > 0
                THEN prev_low - low
            ELSE 0
        END AS minus_dm,
        -- OBV building block
        SIGN(COALESCE(close - prev_close, 0)) * COALESCE(volume, 0) AS signed_volume,
        -- MFI building blocks
        CASE
            WHEN typical_price > prev_typical_price
                THEN typical_price * COALESCE(volume, 0)
            ELSE 0
        END AS positive_money_flow,
        CASE
            WHEN typical_price < prev_typical_price
                THEN typical_price * COALESCE(volume, 0)
            ELSE 0
        END AS negative_money_flow
    FROM base
),

windowed AS (
    SELECT
        *,
        {{ ta_rolling('AVG', 'close', 20) }} AS sma_20,
        {{ ta_rolling('AVG', 'close', 50) }} AS sma_50,
        {{ ta_rolling('AVG', 'close', 200) }} AS sma_200,
        {{ ta_rolling('STDDEV', 'close', 20) }} AS stddev_20,
        {{ ta_rolling('AVG', 'typical_price', 20) }} AS sma_tp_20,
        {{ ta_rolling('MIN', 'low', 14) }} AS lowest_low_14,
        {{ ta_rolling('MAX', 'high', 14) }} AS highest_high_14,
        {{ ta_rolling('MAX', 'high', 252) }} AS high_252,
        {{ ta_rolling('MIN', 'low', 252) }} AS low_252,
        {{ ta_rolling_prior('MAX', 'high', 20) }} AS donchian_high_20,
        {{ ta_rolling_prior('MIN', 'low', 20) }} AS donchian_low_20,
        {{ ta_rolling('AVG', 'volume', 20) }} AS volume_sma_20,
        {{ ta_rolling('SUM', 'positive_money_flow', 14) }} AS positive_mf_14,
        {{ ta_rolling('SUM', 'negative_money_flow', 14) }} AS negative_mf_14,
        SUM(signed_volume) OVER (
            PARTITION BY {{ ta_default_partition() }}
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS obv,
        {{ ta_zscore('close', 60) }} AS zscore_60,
        {{ ta_window_array('close', ewm_lookback) }} AS close_arr,
        {{ ta_window_array('gain', ewm_lookback) }} AS gain_arr,
        {{ ta_window_array('loss', ewm_lookback) }} AS loss_arr,
        {{ ta_window_array('true_range', ewm_lookback) }} AS tr_arr,
        {{ ta_window_array('plus_dm', ewm_lookback) }} AS plus_dm_arr,
        {{ ta_window_array('minus_dm', ewm_lookback) }} AS minus_dm_arr,
        {{ ta_window_array('typical_price', 20) }} AS tp_arr_20
    FROM flows
),

ewm_pass_1 AS (
    SELECT
        * EXCEPT (close_arr, gain_arr, loss_arr, tr_arr, plus_dm_arr, minus_dm_arr, tp_arr_20),
        {{ ta_ewm_from_array('close_arr', 2 / 13) }} AS ema_12,
        {{ ta_ewm_from_array('close_arr', 2 / 27) }} AS ema_26,
        {{ ta_ewm_from_array('gain_arr', 1 / 14) }} AS avg_gain_14,
        {{ ta_ewm_from_array('loss_arr', 1 / 14) }} AS avg_loss_14,
        {{ ta_ewm_from_array('tr_arr', 1 / 14) }} AS atr_14,
        {{ ta_ewm_from_array('plus_dm_arr', 1 / 14) }} AS plus_dm_14,
        {{ ta_ewm_from_array('minus_dm_arr', 1 / 14) }} AS minus_dm_14,
        {{ ta_ewm_mean_abs_dev_from_array('tp_arr_20', 'sma_tp_20') }} AS tp_mean_dev_20
    FROM windowed
),

derived_1 AS (
    SELECT
        *,
        ema_12 - ema_26 AS macd_line,
        100 * SAFE_DIVIDE(avg_gain_14, NULLIF(avg_gain_14 + avg_loss_14, 0)) AS rsi_14,
        100 * SAFE_DIVIDE(plus_dm_14, NULLIF(atr_14, 0)) AS plus_di_14,
        100 * SAFE_DIVIDE(minus_dm_14, NULLIF(atr_14, 0)) AS minus_di_14,
        SAFE_DIVIDE(typical_price - sma_tp_20, 0.015 * NULLIF(tp_mean_dev_20, 0)) AS cci_20,
        100 * SAFE_DIVIDE(close - lowest_low_14, NULLIF(highest_high_14 - lowest_low_14, 0)) AS stoch_k_14,
        -100 * SAFE_DIVIDE(highest_high_14 - close, NULLIF(highest_high_14 - lowest_low_14, 0)) AS williams_r_14,
        100 * SAFE_DIVIDE(positive_mf_14, NULLIF(positive_mf_14 + negative_mf_14, 0)) AS mfi_14,
        100 * {{ ta_daily_return('close', 'close_20_ago') }} AS roc_20,
        sma_20 AS bb_middle_20,
        sma_20 + 2 * stddev_20 AS bb_upper_20,
        sma_20 - 2 * stddev_20 AS bb_lower_20,
        SAFE_DIVIDE(close - (sma_20 - 2 * stddev_20), NULLIF(4 * stddev_20, 0)) AS bb_percent_b,
        100 * SAFE_DIVIDE(4 * stddev_20, NULLIF(sma_20, 0)) AS bb_bandwidth
    FROM ewm_pass_1
),

derived_2 AS (
    SELECT
        *,
        100 * SAFE_DIVIDE(
            ABS(plus_di_14 - minus_di_14), NULLIF(plus_di_14 + minus_di_14, 0)
        ) AS dx_14,
        {{ ta_rolling('AVG', 'stoch_k_14', 3) }} AS stoch_d_3,
        {{ ta_range_position('bb_bandwidth', 126) }} AS bb_bandwidth_pctile_126,
        {{ ta_window_array('macd_line', 40) }} AS macd_arr
    FROM derived_1
),

derived_3 AS (
    SELECT
        * EXCEPT (macd_arr),
        {{ ta_ewm_from_array('macd_arr', 2 / 10) }} AS macd_signal,
        {{ ta_window_array('dx_14', ewm_lookback) }} AS dx_arr
    FROM derived_2
),

ewm_pass_2 AS (
    SELECT
        * EXCEPT (dx_arr),
        {{ ta_ewm_from_array('dx_arr', 1 / 14) }} AS adx_14
    FROM derived_3
)

SELECT
    source_universe,
    symbol,
    exchange,
    name,
    asset_type,
    date,
    bars_available,

    -- Prices and returns
    open,
    high,
    low,
    close,
    volume,
    ROUND(daily_return, 6) AS daily_return,
    ROUND(log_return, 6) AS log_return,

    -- Trend overlays (NULL until the full lookback window exists)
    CASE WHEN bars_available >= 20 THEN ROUND(sma_20, 4) END AS sma_20,
    CASE WHEN bars_available >= 50 THEN ROUND(sma_50, 4) END AS sma_50,
    CASE WHEN bars_available >= 200 THEN ROUND(sma_200, 4) END AS sma_200,
    CASE WHEN bars_available >= 12 THEN ROUND(ema_12, 4) END AS ema_12,
    CASE WHEN bars_available >= 26 THEN ROUND(ema_26, 4) END AS ema_26,

    -- Momentum
    CASE WHEN bars_available >= 15 THEN ROUND(rsi_14, 2) END AS rsi_14,
    CASE WHEN bars_available >= 21 THEN ROUND(roc_20, 4) END AS roc_20,
    CASE WHEN bars_available >= 14 THEN ROUND(stoch_k_14, 2) END AS stoch_k_14,
    CASE WHEN bars_available >= 16 THEN ROUND(stoch_d_3, 2) END AS stoch_d_3,
    CASE WHEN bars_available >= 14 THEN ROUND(williams_r_14, 2) END AS williams_r_14,
    CASE WHEN bars_available >= 20 THEN ROUND(cci_20, 2) END AS cci_20,
    CASE WHEN bars_available >= 26 THEN ROUND(macd_line, 4) END AS macd_line,
    CASE WHEN bars_available >= 34 THEN ROUND(macd_signal, 4) END AS macd_signal,
    CASE WHEN bars_available >= 34 THEN ROUND(macd_line - macd_signal, 4) END AS macd_histogram,

    -- Trend strength / directional movement
    CASE WHEN bars_available >= 15 THEN ROUND(plus_di_14, 2) END AS plus_di_14,
    CASE WHEN bars_available >= 15 THEN ROUND(minus_di_14, 2) END AS minus_di_14,
    CASE WHEN bars_available >= 28 THEN ROUND(adx_14, 2) END AS adx_14,

    -- Volatility / range / regime
    ROUND(true_range, 4) AS true_range,
    CASE WHEN bars_available >= 15 THEN ROUND(atr_14, 4) END AS atr_14,
    CASE WHEN bars_available >= 15
        THEN ROUND(100 * SAFE_DIVIDE(atr_14, NULLIF(close, 0)), 4) END AS natr_14,
    CASE WHEN bars_available >= 20 THEN ROUND(bb_middle_20, 4) END AS bb_middle_20,
    CASE WHEN bars_available >= 20 THEN ROUND(bb_upper_20, 4) END AS bb_upper_20,
    CASE WHEN bars_available >= 20 THEN ROUND(bb_lower_20, 4) END AS bb_lower_20,
    CASE WHEN bars_available >= 20 THEN ROUND(bb_percent_b, 4) END AS bb_percent_b,
    CASE WHEN bars_available >= 20 THEN ROUND(bb_bandwidth, 4) END AS bb_bandwidth,
    CASE WHEN bars_available >= 126 THEN ROUND(bb_bandwidth_pctile_126, 4) END AS bb_bandwidth_pctile_126,
    CASE WHEN bars_available >= 60 THEN ROUND(zscore_60, 2) END AS zscore_60,
    CASE WHEN bars_available >= 252 THEN ROUND(high_252, 4) END AS high_252,
    CASE WHEN bars_available >= 252 THEN ROUND(low_252, 4) END AS low_252,
    CASE WHEN bars_available >= 252
        THEN ROUND(SAFE_DIVIDE(close, NULLIF(high_252, 0)) - 1, 4) END AS pct_from_52w_high,

    -- Volume / money flow
    obv,
    CASE WHEN bars_available >= 20 THEN ROUND(volume_sma_20, 2) END AS volume_sma_20,
    CASE WHEN bars_available >= 20
        THEN ROUND(SAFE_DIVIDE(volume, NULLIF(volume_sma_20, 0)), 4) END AS relative_volume,
    CASE WHEN bars_available >= 15 THEN ROUND(mfi_14, 2) END AS mfi_14,

    -- Breakout levels (prior-bar channel, excludes today)
    CASE WHEN bars_available >= 21 THEN ROUND(donchian_high_20, 4) END AS donchian_high_20,
    CASE WHEN bars_available >= 21 THEN ROUND(donchian_low_20, 4) END AS donchian_low_20

FROM ewm_pass_2
