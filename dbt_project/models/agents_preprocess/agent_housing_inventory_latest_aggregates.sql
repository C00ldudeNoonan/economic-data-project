{{
    config(
        tags=['agents_preprocess']
    )
}}

select
    series_code,
    series_name,
    month,
    current_value,
    pct_change_3m,
    pct_change_6m,
    pct_change_1y,
    date_grain
from {{ ref('housing_inventory_latest_aggregates') }}
where current_value is not null
