{{
    config(
        tags=['agents_preprocess']
    )
}}

select
    date,
    fci,
    equity_score,
    housing_score,
    "10yr_score" as treasury_10yr_score
from {{ source('staging', 'financial_conditions_index') }}
where fci is not null
