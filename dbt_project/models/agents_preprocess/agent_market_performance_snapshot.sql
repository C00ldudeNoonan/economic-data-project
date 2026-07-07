{{
    config(
        materialized='incremental',
        unique_key=['snapshot_date', 'market_category', 'symbol', 'asset_type', 'time_period'],
        incremental_strategy='merge',
        tags=['agents_preprocess']
    )
}}

with sector_snapshot as (
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
        snapshot_date
    from {{ ref('us_sector_summary_snapshot') }}
    {% if is_incremental() %}
    where snapshot_date >= COALESCE(
        (select max(snapshot_date) from {{ this }}),
        DATE '1900-01-01'
    ) - INTERVAL 1 MONTH
    {% endif %}
),

major_index_snapshot as (
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
        DATE_TRUNC(period_end_date, MONTH) as snapshot_date
    from {{ ref('major_indices_summary') }}
    {% if is_incremental() %}
    where DATE_TRUNC(period_end_date, MONTH) >= COALESCE(
        (select max(snapshot_date) from {{ this }}),
        DATE '1900-01-01'
    ) - INTERVAL 1 MONTH
    {% endif %}
),

combined_snapshots as (
    select * from sector_snapshot

    union all

    select * from major_index_snapshot
)

select *
from combined_snapshots
qualify row_number() over (
    partition by snapshot_date, market_category, symbol, asset_type, time_period
    order by period_end_date desc, period_start_date desc
) = 1
