{{ config(
    description='Agent-friendly reliability summary of historical technical signals by universe, symbol, and signal (issue #109, phase 1)'
) }}

/*
    Agent Technical Signal History

    Aggregates technical_signal_instances into signal-reliability stats
    at two grains (flagged by aggregation_grain):
    - 'universe_signal':        source_universe × signal
    - 'universe_symbol_signal': source_universe × symbol × signal

    Hit rates are benchmark-relative (see worked_* definitions in
    technical_signal_instances) and computed only over instances old
    enough to be evaluable at each horizon.
*/

{% set grains = [
    {'label': 'universe_signal', 'symbol_expr': "'ALL'", 'exchange_expr': "'ALL'"},
    {'label': 'universe_symbol_signal', 'symbol_expr': 'symbol', 'exchange_expr': 'exchange'},
] %}

{% for grain in grains %}
SELECT
    '{{ grain.label }}' AS aggregation_grain,
    source_universe,
    {{ grain.symbol_expr }} AS symbol,
    {{ grain.exchange_expr }} AS exchange,
    indicator_name,
    signal_name,
    signal_side,
    COUNT(*) AS total_triggers,
    MIN(entry_date) AS first_trigger_date,
    MAX(entry_date) AS last_trigger_date,
    COUNTIF(worked_21d IS NOT NULL) AS evaluable_21d,
    ROUND(AVG(CAST(worked_5d AS INT64)), 4) AS hit_rate_5d,
    ROUND(AVG(CAST(worked_21d AS INT64)), 4) AS hit_rate_21d,
    ROUND(AVG(CAST(worked_63d AS INT64)), 4) AS hit_rate_63d,
    ROUND(AVG(forward_return_21d), 6) AS avg_forward_return_21d,
    ROUND(AVG(relative_forward_return_21d), 6) AS avg_relative_return_21d,
    ROUND(AVG(relative_forward_return_63d), 6) AS avg_relative_return_63d,
    ROUND(AVG(max_favorable_excursion_21d), 6) AS avg_mfe_21d,
    ROUND(AVG(max_adverse_excursion_21d), 6) AS avg_mae_21d,
    ROUND(AVG(CASE WHEN volume_confirmed THEN CAST(worked_21d AS INT64) END), 4)
        AS hit_rate_21d_volume_confirmed
FROM {{ ref('technical_signal_instances') }}
GROUP BY
    source_universe,
    {% if grain.label == 'universe_symbol_signal' %}symbol, exchange,{% endif %}
    indicator_name,
    signal_name,
    signal_side
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
