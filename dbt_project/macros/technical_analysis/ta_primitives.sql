{#
    Technical-analysis window primitives (issue #109).

    Conventions:
    - Every macro accepts partition_by / order_by instead of hardcoding
      symbol or date. Default partition is the symbol-level grain of
      technical_price_universe: source_universe, symbol, exchange.
    - Warmup handling is the caller's responsibility: gate outputs with
      `CASE WHEN bars_available >= <window> THEN ... END` so indicators
      are NULL until enough lookback rows exist.
#}

{% macro ta_default_partition() %}
    {{- return('source_universe, symbol, exchange') -}}
{% endmacro %}


{% macro ta_rolling(agg, column, window, partition_by=none, order_by='date') %}
    {#- Rolling aggregate over the trailing `window` rows (inclusive of current). -#}
    {%- set partition_by = partition_by or ta_default_partition() -%}
    {{ agg }}({{ column }}) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ window - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}


{% macro ta_rolling_prior(agg, column, window, partition_by=none, order_by='date') %}
    {#- Rolling aggregate over `window` rows ENDING at the prior row.
        Used for breakout levels (e.g. Donchian) so today's bar cannot
        confirm its own breakout. -#}
    {%- set partition_by = partition_by or ta_default_partition() -%}
    {{ agg }}({{ column }}) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ window }} PRECEDING AND 1 PRECEDING
    )
{% endmacro %}


{% macro ta_lag(column, n=1, partition_by=none, order_by='date') %}
    {%- set partition_by = partition_by or ta_default_partition() -%}
    LAG({{ column }}, {{ n }}) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
    )
{% endmacro %}


{% macro ta_lead(column, n=1, partition_by=none, order_by='date') %}
    {%- set partition_by = partition_by or ta_default_partition() -%}
    LEAD({{ column }}, {{ n }}) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
    )
{% endmacro %}


{% macro ta_zscore(column, window, partition_by=none, order_by='date') %}
    {%- set partition_by = partition_by or ta_default_partition() -%}
    SAFE_DIVIDE(
        {{ column }} - {{ ta_rolling('AVG', column, window, partition_by, order_by) }},
        NULLIF({{ ta_rolling('STDDEV', column, window, partition_by, order_by) }}, 0)
    )
{% endmacro %}


{% macro ta_range_position(column, window, partition_by=none, order_by='date') %}
    {#- Position of the current value within the trailing rolling range:
        0.0 at the rolling minimum, 1.0 at the rolling maximum. -#}
    {%- set partition_by = partition_by or ta_default_partition() -%}
    SAFE_DIVIDE(
        {{ column }} - {{ ta_rolling('MIN', column, window, partition_by, order_by) }},
        NULLIF(
            {{ ta_rolling('MAX', column, window, partition_by, order_by) }}
            - {{ ta_rolling('MIN', column, window, partition_by, order_by) }},
            0
        )
    )
{% endmacro %}


{% macro ta_window_array(column, lookback, partition_by=none, order_by='date') %}
    {#- Trailing window of values as an ARRAY of single-field STRUCTs,
        oldest first, for use with ta_ewm_from_array. Values are wrapped
        in a STRUCT because BigQuery arrays cannot hold NULL elements and
        analytic ARRAY_AGG does not support IGNORE NULLS; NULL values are
        filtered inside the consuming subquery instead, which keeps each
        element's recency offset intact. -#}
    {%- set partition_by = partition_by or ta_default_partition() -%}
    ARRAY_AGG(STRUCT({{ column }} AS v)) OVER (
        PARTITION BY {{ partition_by }}
        ORDER BY {{ order_by }}
        ROWS BETWEEN {{ lookback - 1 }} PRECEDING AND CURRENT ROW
    )
{% endmacro %}


{% macro ta_ewm_from_array(array_col, alpha) %}
    {#- Exponentially weighted mean of a trailing-window array built by
        ta_window_array (oldest element first, newest last).

        This is the project-standard EMA implementation for BigQuery
        (issue #109 open question): a finite-window, weight-normalized
        approximation of the recursive EMA. With a lookback of >= 4/alpha
        rows the truncated tail carries < 2% of total weight, so results
        match the recursive definition to well within float noise.

        Weights are proportional to (1-alpha)^(-offset); dividing by the
        weight sum makes the constant factor cancel, so no array-length
        term is needed.

        EMA(span):  alpha = 2 / (span + 1)
        RMA/Wilder(n): alpha = 1 / n
    -#}
    (
        SELECT
            SAFE_DIVIDE(
                SUM(_e.v * POW({{ 1 - alpha }}, -_off)),
                NULLIF(SUM(POW({{ 1 - alpha }}, -_off)), 0)
            )
        FROM UNNEST({{ array_col }}) AS _e WITH OFFSET AS _off
        WHERE _e.v IS NOT NULL
    )
{% endmacro %}


{% macro ta_ewm_mean_abs_dev_from_array(array_col, reference) %}
    {#- Mean absolute deviation of array values from a fixed reference
        (used by CCI, which measures dispersion around the CURRENT SMA). -#}
    (
        SELECT AVG(ABS(_e.v - {{ reference }}))
        FROM UNNEST({{ array_col }}) AS _e
    )
{% endmacro %}
