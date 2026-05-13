{{
    config(
        tags=['agents_preprocess']
    )
}}

select
    symbol,
    symbol as ticker,
    asset_type,
    time_period,
    exchange,
    name,
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
    'sector' as market_category,
    cast(null as date) as snapshot_date
from {{ ref('us_sector_summary') }}

union all

select
    symbol,
    symbol as ticker,
    asset_type,
    time_period,
    exchange,
    name,
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
    'major_index' as market_category,
    cast(null as date) as snapshot_date
from {{ ref('major_indicies_summary') }}
