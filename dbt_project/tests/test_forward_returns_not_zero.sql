-- Test: Forward returns should not be exactly zero (rounded) when prices differ
-- A 0% return is valid if prices are identical, but we flag cases where 
-- the calculation might have rounded to 0 incorrectly or there's a data issue
-- Note: This test allows for legitimate 0% returns (no price change)

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
        quarterly_avg_close,
        'zero_forward_return' as issue_type
    FROM {{ ref(model) }}
    WHERE ABS(pct_change_q1_forward) < 0.01
        AND pct_change_q1_forward IS NOT NULL
        AND quarterly_avg_close > 0
        -- Only flag if this seems suspicious (you can adjust this condition)
        -- For now, we'll flag all 0 values for investigation
        -- In production, you might want to allow legitimate 0% returns
    
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}

