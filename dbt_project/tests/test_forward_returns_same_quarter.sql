-- Test: Months within the same quarter should have the same forward return values
-- This test catches the LEAD() window function bug where forward returns were calculated incorrectly

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
        year_val,
        quarter_num,
        COUNT(DISTINCT pct_change_q1_forward) as distinct_q1_values,
        COUNT(*) as total_rows
    FROM {{ ref(model) }}
    WHERE pct_change_q1_forward IS NOT NULL
    GROUP BY symbol, exchange, year_val, quarter_num
    HAVING COUNT(DISTINCT pct_change_q1_forward) > 1
    
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}

