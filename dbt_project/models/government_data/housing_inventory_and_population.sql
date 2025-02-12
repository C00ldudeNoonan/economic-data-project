{{ config(
    materialized='table'
) }}


With hs as (
SELECT
time_date,
cast(series_value as float) as number_of_households,
EXTRACT(YEAR FROM time_date) AS year
FROM {{ref('housing_inventory')}}
WHERE category_code = 'TTLHH'
and series_value <> '.'
)



SELECT
    series_name,
    CAST(series_value as float) as series_value,
    CAST((CASE 
        WHEN RIGHT(housing_inventory.time, 2) = 'Q1' THEN (LEFT(housing_inventory.time, 4) || '-01-01')::DATE
        WHEN RIGHT(housing_inventory.time, 2) = 'Q2' THEN (LEFT(housing_inventory.time, 4) || '-04-01')::DATE
        WHEN RIGHT(housing_inventory.time, 2) = 'Q3' THEN (LEFT(housing_inventory.time, 4) || '-07-01')::DATE
        WHEN RIGHT(housing_inventory.time, 2) = 'Q4' THEN (LEFT(housing_inventory.time, 4) || '-10-01')::DATE
    END) as date) as time_date,
    EXTRACT(YEAR FROM     CAST((CASE 
        WHEN RIGHT(housing_inventory.time, 2) = 'Q1' THEN (LEFT(housing_inventory.time, 4) || '-01-01')::DATE
        WHEN RIGHT(housing_inventory.time, 2) = 'Q2' THEN (LEFT(housing_inventory.time, 4) || '-04-01')::DATE
        WHEN RIGHT(housing_inventory.time, 2) = 'Q3' THEN (LEFT(housing_inventory.time, 4) || '-07-01')::DATE
        WHEN RIGHT(housing_inventory.time, 2) = 'Q4' THEN (LEFT(housing_inventory.time, 4) || '-10-01')::DATE
    END) as date)) as year,
        hs.number_of_households
FROM {{ref('housing_inventory')}}
LEFT JOIN hs
    on EXTRACT(YEAR FROM     CAST((CASE 
        WHEN RIGHT(housing_inventory.time, 2) = 'Q1' THEN (LEFT(housing_inventory.time, 4) || '-01-01')::DATE
        WHEN RIGHT(housing_inventory.time, 2) = 'Q2' THEN (LEFT(housing_inventory.time, 4) || '-04-01')::DATE
        WHEN RIGHT(housing_inventory.time, 2) = 'Q3' THEN (LEFT(housing_inventory.time, 4) || '-07-01')::DATE
        WHEN RIGHT(housing_inventory.time, 2) = 'Q4' THEN (LEFT(housing_inventory.time, 4) || '-10-01')::DATE
    END) as date)) = hs.year

Where
    error_data = 'no' 
    and category_code ='ESTIMATE'
    and series_name in ('Renter Occupied Units', 'Owner Occupied Units', 'Total Vacant Housing Units')
