-- Test: Forward returns should not be zero when quarterly_avg_close exists
-- Forward returns should be NULL if no future data exists, not zero
-- Allow very small values (< 0.01%) as these may be legitimate rounding to zero

{{ config(severity='warn') }}

{% set models_to_test = [
    'currency_analysis_return',
    'global_markets_analysis_return',
    'major_indicies_analysis_return',
    'us_sector_analysis_return',
    'fixed_income_analysis_return'
] %}

{% for model in models_to_test %}
    SELECT 
        symbol,
        exchange,
        month_date,
        pct_change_q1_forward,
        quarterly_avg_close
    FROM {{ ref(model) }}
    WHERE ABS(pct_change_q1_forward) < 0.01
        AND pct_change_q1_forward IS NOT NULL
        AND quarterly_avg_close > 0
    
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}

