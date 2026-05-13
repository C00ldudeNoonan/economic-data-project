{{
    config(
        tags=['agents_preprocess']
    )
}}

select
    commodity_name,
    commodity_name as commodity,
    commodity_unit,
    time_period,
    period_start_date,
    period_end_date,
    trading_days,
    total_return_pct,
    avg_daily_return_pct,
    volatility_pct,
    volatility_pct as annualized_volatility_pct,
    win_rate_pct,
    total_price_change,
    avg_daily_price_change,
    worst_day_change,
    worst_day_change as worst_day_pct_change,
    best_day_change,
    best_day_change as best_day_pct_change,
    positive_days,
    negative_days,
    neutral_days,
    period_start_price,
    period_end_price,
    'energy' as commodity_category,
    cast(null as date) as snapshot_date
from {{ ref('energy_commodities_summary') }}

union all

select
    commodity_name,
    commodity_name as commodity,
    commodity_unit,
    time_period,
    period_start_date,
    period_end_date,
    trading_days,
    total_return_pct,
    avg_daily_return_pct,
    volatility_pct,
    volatility_pct as annualized_volatility_pct,
    win_rate_pct,
    total_price_change,
    avg_daily_price_change,
    worst_day_change,
    worst_day_change as worst_day_pct_change,
    best_day_change,
    best_day_change as best_day_pct_change,
    positive_days,
    negative_days,
    neutral_days,
    period_start_price,
    period_end_price,
    'input' as commodity_category,
    cast(null as date) as snapshot_date
from {{ ref('input_commodities_summary') }}

union all

select
    commodity_name,
    commodity_name as commodity,
    commodity_unit,
    time_period,
    period_start_date,
    period_end_date,
    trading_days,
    total_return_pct,
    avg_daily_return_pct,
    volatility_pct,
    volatility_pct as annualized_volatility_pct,
    win_rate_pct,
    total_price_change,
    avg_daily_price_change,
    worst_day_change,
    worst_day_change as worst_day_pct_change,
    best_day_change,
    best_day_change as best_day_pct_change,
    positive_days,
    negative_days,
    neutral_days,
    period_start_price,
    period_end_price,
    'agriculture' as commodity_category,
    cast(null as date) as snapshot_date
from {{ ref('agriculture_commodities_summary') }}
