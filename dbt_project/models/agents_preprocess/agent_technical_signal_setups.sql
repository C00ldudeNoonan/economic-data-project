{{ config(
    description='Agent-friendly view of current technical signal setups and active signals (issue #109, phase 1)'
) }}

/*
    Agent Technical Signal Setups

    One row per symbol × signal currently in a setup/triggered/active
    state, phrased for LLM-agent consumption: descriptive state text and
    the key numbers an agent needs to reason about the setup.
*/

SELECT
    source_universe,
    symbol,
    exchange,
    date AS as_of_date,
    signal_name,
    indicator_name,
    signal_side,
    signal_state,
    CASE signal_state
        WHEN 'setup' THEN 'Pre-trigger condition present; signal has not fired yet'
        WHEN 'triggered' THEN 'Signal fired on the most recent bar'
        WHEN 'active' THEN 'Signal fired recently and is still inside its holding window'
    END AS state_description,
    signal_value,
    close AS last_close,
    relative_volume,
    volume_confirmed,
    trigger_date,
    setup_date,
    bars_since_trigger,
    max_holding_bars
FROM {{ ref('technical_current_setups') }}
