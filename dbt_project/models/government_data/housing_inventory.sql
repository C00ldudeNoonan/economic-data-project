{{ config(
    materialized='table'
) }}


select 
data_type_code as data_code,
seasonally_adj,
category_code,
cast(cell_value as float) as series_value,
error_data,
time,
series_name,
plot_groupings,
CAST((CASE 
    WHEN RIGHT(time, 2) = 'Q1' THEN (LEFT(time, 4) || '-01-01')::DATE
    WHEN RIGHT(time, 2) = 'Q2' THEN (LEFT(time, 4) || '-04-01')::DATE
    WHEN RIGHT(time, 2) = 'Q3' THEN (LEFT(time, 4) || '-07-01')::DATE
    WHEN RIGHT(time, 2) = 'Q4' THEN (LEFT(time, 4) || '-10-01')::DATE
END) as date) as time_date
from {{ ref('stg_housing_inventory') }}
WHERE cell_value <> '(z)'