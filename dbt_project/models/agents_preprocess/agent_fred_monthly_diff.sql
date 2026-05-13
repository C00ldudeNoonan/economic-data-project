{{
    config(
        tags=['agents_preprocess']
    )
}}

select
    series_code,
    series_name,
    date,
    value,
    period_diff,
    data_source
from {{ ref('fred_monthly_diff') }}
