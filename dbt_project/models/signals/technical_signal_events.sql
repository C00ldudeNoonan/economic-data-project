{{ config(
    description='Long-format technical signal event log with setup/triggered/active/completed/expired states (issue #109, phase 1)'
) }}

/*
    Technical Signal Events

    Grain: one row per source_universe, symbol, exchange, date, signal_name
    (rows only exist while a signal is in a non-null state).

    Signal definitions live in the jinja registry below (name, family,
    side, setup/trigger conditions, representative value). Conditions are
    evaluated against technical_indicator_daily plus previous-bar values,
    so no future data is referenced.

    State machine per symbol × signal:
    - setup:     pre-trigger condition present, no recent trigger
    - triggered: the crossing/breakout event fired on this bar
    - active:    a trigger fired within the last max_holding_bars bars
    - completed: exactly max_holding_bars bars have passed since trigger
    - expired:   a setup condition just ended without ever triggering

    Repeat raw triggers inside an active window are recorded as new
    'triggered' rows (standard event-study convention); dedup decisions
    belong to consumers such as technical_signal_instances.
*/

{% set max_holding_bars = 21 %}

{% set signal_registry = [
    {
        'name': 'golden_cross',
        'family': 'sma',
        'side': 'bullish',
        'setup': 'sma_50 < sma_200 AND SAFE_DIVIDE(sma_200 - sma_50, NULLIF(sma_200, 0)) < 0.01',
        'trigger': 'sma_50 > sma_200 AND prev_sma_50 <= prev_sma_200',
        'value': 'SAFE_DIVIDE(sma_50 - sma_200, NULLIF(sma_200, 0))',
    },
    {
        'name': 'death_cross',
        'family': 'sma',
        'side': 'bearish',
        'setup': 'sma_50 > sma_200 AND SAFE_DIVIDE(sma_50 - sma_200, NULLIF(sma_200, 0)) < 0.01',
        'trigger': 'sma_50 < sma_200 AND prev_sma_50 >= prev_sma_200',
        'value': 'SAFE_DIVIDE(sma_50 - sma_200, NULLIF(sma_200, 0))',
    },
    {
        'name': 'price_cross_sma200_up',
        'family': 'sma',
        'side': 'bullish',
        'setup': 'close < sma_200 AND SAFE_DIVIDE(sma_200 - close, NULLIF(sma_200, 0)) < 0.02',
        'trigger': 'close > sma_200 AND prev_close <= prev_sma_200',
        'value': 'SAFE_DIVIDE(close - sma_200, NULLIF(sma_200, 0))',
    },
    {
        'name': 'price_cross_sma200_down',
        'family': 'sma',
        'side': 'bearish',
        'setup': 'close > sma_200 AND SAFE_DIVIDE(close - sma_200, NULLIF(sma_200, 0)) < 0.02',
        'trigger': 'close < sma_200 AND prev_close >= prev_sma_200',
        'value': 'SAFE_DIVIDE(close - sma_200, NULLIF(sma_200, 0))',
    },
    {
        'name': 'rsi_oversold_recovery',
        'family': 'rsi',
        'side': 'bullish',
        'setup': 'rsi_14 < 30',
        'trigger': 'rsi_14 >= 30 AND prev_rsi_14 < 30',
        'value': 'rsi_14',
    },
    {
        'name': 'rsi_overbought_reversal',
        'family': 'rsi',
        'side': 'bearish',
        'setup': 'rsi_14 > 70',
        'trigger': 'rsi_14 <= 70 AND prev_rsi_14 > 70',
        'value': 'rsi_14',
    },
    {
        'name': 'macd_bullish_cross',
        'family': 'macd',
        'side': 'bullish',
        'setup': 'macd_line < macd_signal AND macd_histogram > prev_macd_histogram',
        'trigger': 'macd_line > macd_signal AND prev_macd_line <= prev_macd_signal',
        'value': 'macd_histogram',
    },
    {
        'name': 'macd_bearish_cross',
        'family': 'macd',
        'side': 'bearish',
        'setup': 'macd_line > macd_signal AND macd_histogram < prev_macd_histogram',
        'trigger': 'macd_line < macd_signal AND prev_macd_line >= prev_macd_signal',
        'value': 'macd_histogram',
    },
    {
        'name': 'stoch_oversold_cross',
        'family': 'stochastic',
        'side': 'bullish',
        'setup': 'stoch_k_14 < 20',
        'trigger': 'prev_stoch_k_14 < 20 AND stoch_k_14 > stoch_d_3 AND prev_stoch_k_14 <= prev_stoch_d_3',
        'value': 'stoch_k_14',
    },
    {
        'name': 'stoch_overbought_cross',
        'family': 'stochastic',
        'side': 'bearish',
        'setup': 'stoch_k_14 > 80',
        'trigger': 'prev_stoch_k_14 > 80 AND stoch_k_14 < stoch_d_3 AND prev_stoch_k_14 >= prev_stoch_d_3',
        'value': 'stoch_k_14',
    },
    {
        'name': 'bollinger_squeeze_breakout_up',
        'family': 'bollinger',
        'side': 'bullish',
        'setup': 'bb_bandwidth_pctile_126 <= 0.10 AND close <= bb_upper_20',
        'trigger': 'prev_bb_bandwidth_pctile_126 <= 0.10 AND close > bb_upper_20',
        'value': 'bb_bandwidth_pctile_126',
    },
    {
        'name': 'bollinger_squeeze_breakout_down',
        'family': 'bollinger',
        'side': 'bearish',
        'setup': 'bb_bandwidth_pctile_126 <= 0.10 AND close >= bb_lower_20',
        'trigger': 'prev_bb_bandwidth_pctile_126 <= 0.10 AND close < bb_lower_20',
        'value': 'bb_bandwidth_pctile_126',
    },
    {
        'name': 'donchian_breakout_up',
        'family': 'donchian',
        'side': 'bullish',
        'setup': 'close <= donchian_high_20 AND SAFE_DIVIDE(donchian_high_20 - close, NULLIF(donchian_high_20, 0)) < 0.01',
        'trigger': 'close > donchian_high_20',
        'value': 'SAFE_DIVIDE(close - donchian_high_20, NULLIF(donchian_high_20, 0))',
    },
    {
        'name': 'donchian_breakout_down',
        'family': 'donchian',
        'side': 'bearish',
        'setup': 'close >= donchian_low_20 AND SAFE_DIVIDE(close - donchian_low_20, NULLIF(donchian_low_20, 0)) < 0.01',
        'trigger': 'close < donchian_low_20',
        'value': 'SAFE_DIVIDE(close - donchian_low_20, NULLIF(donchian_low_20, 0))',
    },
] %}

WITH indicators AS (
    SELECT
        source_universe,
        symbol,
        exchange,
        date,
        bars_available,
        close,
        relative_volume,
        sma_50,
        sma_200,
        rsi_14,
        macd_line,
        macd_signal,
        macd_histogram,
        stoch_k_14,
        stoch_d_3,
        bb_upper_20,
        bb_lower_20,
        bb_bandwidth_pctile_126,
        donchian_high_20,
        donchian_low_20,
        {{ ta_lag('close') }} AS prev_close,
        {{ ta_lag('sma_50') }} AS prev_sma_50,
        {{ ta_lag('sma_200') }} AS prev_sma_200,
        {{ ta_lag('rsi_14') }} AS prev_rsi_14,
        {{ ta_lag('macd_line') }} AS prev_macd_line,
        {{ ta_lag('macd_signal') }} AS prev_macd_signal,
        {{ ta_lag('macd_histogram') }} AS prev_macd_histogram,
        {{ ta_lag('stoch_k_14') }} AS prev_stoch_k_14,
        {{ ta_lag('stoch_d_3') }} AS prev_stoch_d_3,
        {{ ta_lag('bb_bandwidth_pctile_126') }} AS prev_bb_bandwidth_pctile_126
    FROM {{ ref('technical_indicator_daily') }}
),

flagged AS (
    {% for signal in signal_registry %}
    SELECT
        source_universe,
        symbol,
        exchange,
        date,
        bars_available,
        close,
        relative_volume,
        '{{ signal.name }}' AS signal_name,
        '{{ signal.family }}' AS indicator_name,
        '{{ signal.side }}' AS signal_side,
        COALESCE({{ signal.setup }}, FALSE) AS is_setup,
        COALESCE({{ signal.trigger }}, FALSE) AS is_trigger,
        {{ signal.value }} AS signal_value
    FROM indicators
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
),

stated AS (
    SELECT
        *,
        MAX(IF(is_trigger, bars_available, NULL)) OVER (
            PARTITION BY source_universe, symbol, exchange, signal_name
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_trigger_bar,
        MAX(IF(is_trigger, date, NULL)) OVER (
            PARTITION BY source_universe, symbol, exchange, signal_name
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_trigger_date,
        MAX(IF(is_setup, date, NULL)) OVER (
            PARTITION BY source_universe, symbol, exchange, signal_name
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS last_setup_date,
        LAG(is_setup) OVER (
            PARTITION BY source_universe, symbol, exchange, signal_name
            ORDER BY date
        ) AS prev_is_setup
    FROM flagged
),

classified AS (
    SELECT
        *,
        bars_available - last_trigger_bar AS bars_since_trigger,
        CASE
            WHEN is_trigger THEN 'triggered'
            WHEN bars_available - last_trigger_bar
                BETWEEN 1 AND {{ max_holding_bars - 1 }} THEN 'active'
            WHEN bars_available - last_trigger_bar = {{ max_holding_bars }} THEN 'completed'
            WHEN is_setup THEN 'setup'
            WHEN COALESCE(prev_is_setup, FALSE) AND NOT is_setup THEN 'expired'
        END AS signal_state
    FROM stated
)

SELECT
    source_universe,
    symbol,
    exchange,
    date,
    indicator_name,
    signal_name,
    signal_side,
    signal_state,
    ROUND(signal_value, 6) AS signal_value,
    close,
    relative_volume,
    COALESCE(relative_volume >= 1.5, FALSE) AS volume_confirmed,
    is_setup,
    is_trigger,
    last_trigger_date AS trigger_date,
    last_setup_date AS setup_date,
    bars_since_trigger,
    {{ max_holding_bars }} AS max_holding_bars
FROM classified
WHERE signal_state IS NOT NULL
