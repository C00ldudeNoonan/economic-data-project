-- Test: Check for unexpected inconsistencies in forward returns
-- This test flags cases where Q1 forward exists but Q2 is missing (unexpected)
-- Note: It's normal for Q2/Q3/Q4 to be missing at the end of the dataset, so we exclude recent quarters

{% set models_to_test = [
    'currency_analysis_return',
    'global_markets_analysis_return',
    'major_indicies_analysis_return',
    'us_sector_analysis_return',
    'fixed_income_analysis_return'
] %}

WITH all_model_results AS (
    {% for model in models_to_test %}
        SELECT 
            t.symbol,
            t.exchange,
            t.year_val,
            t.quarter_num,
            t.month_date,
            t.pct_change_q1_forward,
            t.pct_change_q2_forward,
            'q2_missing_but_q1_exists' as inconsistency_type,
            '{{ model }}' as model_name
        FROM {{ ref(model) }} t
        CROSS JOIN (
            SELECT MAX(month_date) as latest_date
            FROM {{ ref(model) }}
        ) m
        WHERE t.pct_change_q1_forward IS NOT NULL
            AND t.pct_change_q2_forward IS NULL
            AND t.month_date < DATE_TRUNC('year', m.latest_date)
        
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)
SELECT 
    symbol,
    exchange,
    year_val,
    quarter_num,
    month_date,
    pct_change_q1_forward,
    pct_change_q2_forward,
    inconsistency_type,
    model_name
FROM all_model_results

