{{ config(
    description='Historical evaluation of triggered technical signals: forward returns, excursions, and worked labels (issue #109, phase 1)'
) }}

/*
    Technical Signal Instances

    Grain: one row per source_universe, symbol, exchange, signal_name,
    entry_date (every 'triggered' event from technical_signal_events).

    Forward returns are computed on each symbol's own trading calendar
    via LEAD offsets, so future data appears ONLY in this evaluation
    model — never in the events/setups models.

    Benchmark: SPY forward returns over the same horizons, joined on the
    entry date. All covered instruments are US-listed, so trading
    calendars align; any residual drift from symbol-specific halts is a
    documented approximation.

    "Worked" labels are benchmark-relative: a bullish signal worked at a
    horizon when the symbol beat SPY, a bearish signal worked when it
    underperformed SPY. Absolute returns are also provided so consumers
    can apply their own definition.
*/

{% set horizons = [1, 5, 10, 21, 63, 126] %}
{% set excursion_bars = 21 %}

WITH spine AS (
    SELECT
        source_universe,
        symbol,
        exchange,
        date,
        close,
        {% for h in horizons %}
        {{ ta_lead('close', h) }} AS fwd_close_{{ h }},
        {% endfor %}
        MAX(high) OVER (
            PARTITION BY {{ ta_default_partition() }}
            ORDER BY date
            ROWS BETWEEN 1 FOLLOWING AND {{ excursion_bars }} FOLLOWING
        ) AS max_high_fwd,
        MIN(low) OVER (
            PARTITION BY {{ ta_default_partition() }}
            ORDER BY date
            ROWS BETWEEN 1 FOLLOWING AND {{ excursion_bars }} FOLLOWING
        ) AS min_low_fwd
    FROM {{ ref('technical_price_universe') }}
),

benchmark AS (
    SELECT
        date,
        {% for h in horizons %}
        SAFE_DIVIDE(fwd_close_{{ h }}, NULLIF(close, 0)) - 1 AS spy_fwd_return_{{ h }}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM spine
    WHERE source_universe = 'major_index'
      AND symbol = 'SPY'
),

triggers AS (
    SELECT
        source_universe,
        symbol,
        exchange,
        date AS entry_date,
        indicator_name,
        signal_name,
        signal_side,
        signal_value,
        close AS entry_price,
        relative_volume,
        volume_confirmed
    FROM {{ ref('technical_signal_events') }}
    WHERE signal_state = 'triggered'
),

evaluated AS (
    SELECT
        t.*,
        {% for h in horizons %}
        SAFE_DIVIDE(s.fwd_close_{{ h }}, NULLIF(t.entry_price, 0)) - 1 AS forward_return_{{ h }}d,
        b.spy_fwd_return_{{ h }} AS benchmark_forward_return_{{ h }}d,
        (SAFE_DIVIDE(s.fwd_close_{{ h }}, NULLIF(t.entry_price, 0)) - 1)
            - b.spy_fwd_return_{{ h }} AS relative_forward_return_{{ h }}d,
        {% endfor %}
        SAFE_DIVIDE(s.max_high_fwd, NULLIF(t.entry_price, 0)) - 1 AS max_favorable_excursion_{{ excursion_bars }}d,
        SAFE_DIVIDE(s.min_low_fwd, NULLIF(t.entry_price, 0)) - 1 AS max_adverse_excursion_{{ excursion_bars }}d
    FROM triggers AS t
    INNER JOIN spine AS s
        ON t.source_universe = s.source_universe
        AND t.symbol = s.symbol
        AND t.exchange = s.exchange
        AND t.entry_date = s.date
    LEFT JOIN benchmark AS b
        ON t.entry_date = b.date
)

SELECT
    source_universe,
    symbol,
    exchange,
    indicator_name,
    signal_name,
    signal_side,
    entry_date,
    ROUND(entry_price, 4) AS entry_price,
    signal_value,
    ROUND(relative_volume, 4) AS relative_volume,
    volume_confirmed,
    {% for h in horizons %}
    ROUND(forward_return_{{ h }}d, 6) AS forward_return_{{ h }}d,
    ROUND(benchmark_forward_return_{{ h }}d, 6) AS benchmark_forward_return_{{ h }}d,
    ROUND(relative_forward_return_{{ h }}d, 6) AS relative_forward_return_{{ h }}d,
    {% endfor %}
    ROUND(max_favorable_excursion_{{ excursion_bars }}d, 6) AS max_favorable_excursion_{{ excursion_bars }}d,
    ROUND(max_adverse_excursion_{{ excursion_bars }}d, 6) AS max_adverse_excursion_{{ excursion_bars }}d,
    -- MFE for bearish signals is downside capture; swap excursions by side
    CASE
        WHEN signal_side = 'bearish'
            THEN ROUND(-max_adverse_excursion_{{ excursion_bars }}d, 6)
        ELSE ROUND(max_favorable_excursion_{{ excursion_bars }}d, 6)
    END AS side_adjusted_mfe_{{ excursion_bars }}d,
    {% for h in [5, 21, 63] %}
    CASE
        WHEN relative_forward_return_{{ h }}d IS NULL THEN NULL
        WHEN signal_side = 'bullish' THEN relative_forward_return_{{ h }}d > 0
        WHEN signal_side = 'bearish' THEN relative_forward_return_{{ h }}d < 0
    END AS worked_{{ h }}d{% if not loop.last %},{% endif %}
    {% endfor %}
FROM evaluated
