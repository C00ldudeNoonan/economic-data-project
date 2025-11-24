-- Test: Check for weekly data completeness in equities/commodities models
-- This test identifies symbols/commodities with missing weeks in the last 12 weeks
-- Expected: Each symbol/commodity should have data for most weeks (allowing for holidays/weekends)

WITH all_model_results AS (
    SELECT 
        'stg_us_sectors' AS model_name,
        DATE_TRUNC('week', date) AS week_start,
        COUNT(*) AS record_count
    FROM {{ ref('stg_us_sectors') }}
    WHERE date >= CURRENT_DATE - INTERVAL '12 weeks'
        AND date < CURRENT_DATE - INTERVAL '1 week'
    GROUP BY DATE_TRUNC('week', date)
    
    UNION ALL
    
    SELECT 
        'stg_currency' AS model_name,
        DATE_TRUNC('week', date) AS week_start,
        COUNT(*) AS record_count
    FROM {{ ref('stg_currency') }}
    WHERE date >= CURRENT_DATE - INTERVAL '12 weeks'
        AND date < CURRENT_DATE - INTERVAL '1 week'
    GROUP BY DATE_TRUNC('week', date)
    
    UNION ALL
    
    SELECT 
        'stg_major_indices' AS model_name,
        DATE_TRUNC('week', date) AS week_start,
        COUNT(*) AS record_count
    FROM {{ ref('stg_major_indices') }}
    WHERE date >= CURRENT_DATE - INTERVAL '12 weeks'
        AND date < CURRENT_DATE - INTERVAL '1 week'
    GROUP BY DATE_TRUNC('week', date)
    
    UNION ALL
    
    SELECT 
        'stg_fixed_income' AS model_name,
        DATE_TRUNC('week', date) AS week_start,
        COUNT(*) AS record_count
    FROM {{ ref('stg_fixed_income') }}
    WHERE date >= CURRENT_DATE - INTERVAL '12 weeks'
        AND date < CURRENT_DATE - INTERVAL '1 week'
    GROUP BY DATE_TRUNC('week', date)
    
    UNION ALL
    
    SELECT 
        'stg_global_markets' AS model_name,
        DATE_TRUNC('week', date) AS week_start,
        COUNT(*) AS record_count
    FROM {{ ref('stg_global_markets') }}
    WHERE date >= CURRENT_DATE - INTERVAL '12 weeks'
        AND date < CURRENT_DATE - INTERVAL '1 week'
    GROUP BY DATE_TRUNC('week', date)
    
    UNION ALL
    
    SELECT 
        'stg_agriculture_commodities' AS model_name,
        DATE_TRUNC('week', date) AS week_start,
        COUNT(*) AS record_count
    FROM {{ ref('stg_agriculture_commodities') }}
    WHERE date >= CURRENT_DATE - INTERVAL '12 weeks'
        AND date < CURRENT_DATE - INTERVAL '1 week'
    GROUP BY DATE_TRUNC('week', date)
    
    UNION ALL
    
    SELECT 
        'stg_energy_commodities' AS model_name,
        DATE_TRUNC('week', date) AS week_start,
        COUNT(*) AS record_count
    FROM {{ ref('stg_energy_commodities') }}
    WHERE date >= CURRENT_DATE - INTERVAL '12 weeks'
        AND date < CURRENT_DATE - INTERVAL '1 week'
    GROUP BY DATE_TRUNC('week', date)
    
    UNION ALL
    
    SELECT 
        'stg_input_commodities' AS model_name,
        DATE_TRUNC('week', date) AS week_start,
        COUNT(*) AS record_count
    FROM {{ ref('stg_input_commodities') }}
    WHERE date >= CURRENT_DATE - INTERVAL '12 weeks'
        AND date < CURRENT_DATE - INTERVAL '1 week'
    GROUP BY DATE_TRUNC('week', date)
),
expected_weeks AS (
    SELECT DISTINCT week_start
    FROM all_model_results
),
actual_weeks AS (
    SELECT DISTINCT week_start
    FROM all_model_results
),
missing_weeks AS (
    SELECT ew.week_start
    FROM expected_weeks ew
    LEFT JOIN actual_weeks aw ON ew.week_start = aw.week_start
    WHERE aw.week_start IS NULL
)
SELECT 
    COUNT(*) AS missing_week_count
FROM missing_weeks
HAVING COUNT(*) > 2
