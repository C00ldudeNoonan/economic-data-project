-- Test: Check for yearly data completeness in economic data models
-- This test ensures each series/indicator has at least one value per year
-- Expected: Each series should have at least one data point per year for the last 5 years

WITH all_model_results AS (
    SELECT 
        'stg_fred_series' AS model_name,
        series_code AS identifier,
        EXTRACT(YEAR FROM date) AS year_val
    FROM {{ ref('stg_fred_series') }}
    WHERE date >= CURRENT_DATE - INTERVAL '5 years'
    
    UNION ALL
    
    SELECT 
        'stg_housing_inventory' AS model_name,
        'housing' AS identifier,
        CAST(LEFT(time, 4) AS INTEGER) AS year_val
    FROM {{ ref('stg_housing_inventory') }}
    WHERE CAST(LEFT(time, 4) AS INTEGER) >= EXTRACT(YEAR FROM CURRENT_DATE) - 5
    
    UNION ALL
    
    SELECT 
        'stg_housing_pulse' AS model_name,
        'housing_pulse' AS identifier,
        CAST(LEFT(TIME, 4) AS INTEGER) AS year_val
    FROM {{ ref('stg_housing_pulse') }}
    WHERE CAST(LEFT(TIME, 4) AS INTEGER) >= EXTRACT(YEAR FROM CURRENT_DATE) - 5
    
    UNION ALL
    
    SELECT 
        'stg_treasury_yields' AS model_name,
        'treasury' AS identifier,
        EXTRACT(YEAR FROM date) AS year_val
    FROM {{ ref('stg_treasury_yields') }}
    WHERE date >= CURRENT_DATE - INTERVAL '5 years'
),
expected_years AS (
    SELECT DISTINCT
        model_name,
        identifier,
        year_val
    FROM all_model_results
),
actual_years AS (
    SELECT DISTINCT
        model_name,
        identifier,
        year_val
    FROM all_model_results
),
missing_years AS (
    SELECT 
        ey.model_name,
        ey.identifier,
        ey.year_val
    FROM expected_years ey
    LEFT JOIN actual_years ay 
        ON ey.model_name = ay.model_name
        AND ey.identifier = ay.identifier
        AND ey.year_val = ay.year_val
    WHERE ay.year_val IS NULL
)
SELECT 
    model_name,
    identifier,
    year_val AS missing_year
FROM missing_years
