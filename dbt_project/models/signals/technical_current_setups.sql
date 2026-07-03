{{ config(
    description='Latest-bar technical signal setups and active signals per symbol (issue #109, phase 1)'
) }}

/*
    Technical Current Setups

    Grain: one row per source_universe, symbol, exchange, signal_name at
    each symbol's most recent trading bar.

    Answers "what is happening right now?": which symbols are in a setup
    phase, just triggered, or still inside an active signal window.
    Symbols whose data is more than 14 calendar days stale are excluded
    so dead listings don't linger as false setups.

    Contains no forward-looking fields by construction (see
    technical_signal_events).
*/

WITH latest_bar AS (
    SELECT
        source_universe,
        symbol,
        exchange,
        MAX(date) AS latest_date
    FROM {{ ref('technical_indicator_daily') }}
    GROUP BY source_universe, symbol, exchange
    HAVING MAX(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
)

SELECT
    e.source_universe,
    e.symbol,
    e.exchange,
    e.date,
    e.indicator_name,
    e.signal_name,
    e.signal_side,
    e.signal_state,
    e.signal_value,
    e.close,
    e.relative_volume,
    e.volume_confirmed,
    e.trigger_date,
    e.setup_date,
    e.bars_since_trigger,
    e.max_holding_bars
FROM {{ ref('technical_signal_events') }} AS e
INNER JOIN latest_bar AS l
    ON e.source_universe = l.source_universe
    AND e.symbol = l.symbol
    AND e.exchange = l.exchange
    AND e.date = l.latest_date
WHERE e.signal_state IN ('setup', 'triggered', 'active')
