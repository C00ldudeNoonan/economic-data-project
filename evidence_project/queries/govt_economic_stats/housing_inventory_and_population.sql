-- need to have a query that incorporates the population data to the housing inventory data so we can see inventory and population data together
With hs as (
SELECT
date as time_date,
cast(value as float) as number_of_households,
EXTRACT(YEAR FROM date) AS year
FROM housing_inventory
WHERE series_code = 'TTLHH'
and value <> '.'
)



SELECT
    series_name,
    CAST(cell_value as float) as series_value,
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
FROM housing_inventory
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
