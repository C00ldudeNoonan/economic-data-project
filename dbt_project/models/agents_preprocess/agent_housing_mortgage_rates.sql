{{
    config(
        tags=['agents_preprocess']
    )
}}

select
    date,
    mortgage_rate,
    median_price_no_down_payment,
    median_price_20_pct_down_payment,
    monthly_payment_no_down_payment,
    monthly_payment_20_pct_down_payment
from {{ ref('housing_mortgage_rates') }}
