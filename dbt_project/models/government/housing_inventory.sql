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
    plot_grouping,
    cast((case
        when
            right(time, 2) = 'Q1'
            then cast((left(time, 4) || '-01-01') as date)
        when
            right(time, 2) = 'Q2'
            then cast((left(time, 4) || '-04-01') as date)
        when
            right(time, 2) = 'Q3'
            then cast((left(time, 4) || '-07-01') as date)
        when
            right(time, 2) = 'Q4'
            then cast((left(time, 4) || '-10-01') as date)
    end) as date) as time_date
from {{ ref('stg_housing_inventory') }}
where cell_value <> '(z)'
