{#
    Price-transform expressions (issue #109). These are plain column
    expressions — no window functions — so they can be used at any stage.
#}

{% macro ta_typical_price(high='high', low='low', close='close') %}
    (({{ high }} + {{ low }} + {{ close }}) / 3)
{% endmacro %}


{% macro ta_median_price(high='high', low='low') %}
    (({{ high }} + {{ low }}) / 2)
{% endmacro %}


{% macro ta_weighted_close(high='high', low='low', close='close') %}
    (({{ high }} + {{ low }} + 2 * {{ close }}) / 4)
{% endmacro %}


{% macro ta_true_range(high='high', low='low', prev_close='prev_close') %}
    {#- prev_close is expected to be a precomputed LAG(close) column.
        Falls back to plain high-low on the first bar (prev_close NULL),
        matching TA-Lib behavior. -#}
    GREATEST(
        {{ high }} - {{ low }},
        COALESCE(ABS({{ high }} - {{ prev_close }}), {{ high }} - {{ low }}),
        COALESCE(ABS({{ low }} - {{ prev_close }}), {{ high }} - {{ low }})
    )
{% endmacro %}


{% macro ta_daily_return(close='close', prev_close='prev_close') %}
    SAFE_DIVIDE({{ close }} - {{ prev_close }}, NULLIF({{ prev_close }}, 0))
{% endmacro %}


{% macro ta_log_return(close='close', prev_close='prev_close') %}
    SAFE.LN(SAFE_DIVIDE({{ close }}, NULLIF({{ prev_close }}, 0)))
{% endmacro %}
