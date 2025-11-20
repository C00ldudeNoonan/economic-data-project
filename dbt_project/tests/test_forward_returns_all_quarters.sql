-- Test: Check that forward returns are calculated for all forward quarters consistently
-- This ensures Q1, Q2, Q3, Q4 forward returns follow the same pattern

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
        month_date,
        CASE 
            WHEN pct_change_q1_forward IS NOT NULL AND pct_change_q2_forward IS NULL THEN 'q2_missing'
            WHEN pct_change_q1_forward IS NOT NULL AND pct_change_q3_forward IS NULL THEN 'q3_missing'
            WHEN pct_change_q1_forward IS NOT NULL AND pct_change_q4_forward IS NULL THEN 'q4_missing'
            ELSE NULL
        END as inconsistency_type
    FROM {{ ref(model) }}
    WHERE pct_change_q1_forward IS NOT NULL
        AND (
            pct_change_q2_forward IS NULL 
            OR pct_change_q3_forward IS NULL 
            OR pct_change_q4_forward IS NULL
        )
    
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}

