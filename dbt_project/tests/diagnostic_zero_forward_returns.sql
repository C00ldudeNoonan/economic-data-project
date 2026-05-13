-- Diagnostic query to investigate zero forward returns
-- Run this to see the actual rows that are failing the test
-- This will help determine if they're legitimate 0% returns or data quality issues

{% set models_to_test = [
    'currency_analysis_return',
    'global_markets_analysis_return',
    'major_indicies_analysis_return',
    'us_sector_analysis_return',
    'fixed_income_analysis_return'
] %}

{% for model in models_to_test %}
    SELECT 
        '{{ model }}' as model_name,
        symbol,
        exchange,
        month_date,
        year_val,
        quarter_num,
        quarterly_avg_close,
        pct_change_q1_forward,
        pct_change_q2_forward,
        pct_change_q3_forward,
        pct_change_q4_forward,
        quarterly_avg_close as expected_forward_price_for_zero_return
    FROM {{ ref(model) }}
    WHERE pct_change_q1_forward = 0 
        AND quarterly_avg_close > 0
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}

ORDER BY model_name, symbol, exchange, month_date

