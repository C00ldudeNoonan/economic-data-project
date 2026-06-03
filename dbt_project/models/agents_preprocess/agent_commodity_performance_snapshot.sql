{{
    config(
        materialized='incremental',
        unique_key=['snapshot_date', 'commodity_category', 'commodity_name', 'commodity_unit', 'time_period'],
        incremental_strategy='delete+insert',
        tags=['agents_preprocess']
    )
}}

with energy_snapshot as (
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
        snapshot_date
    from {{ ref('energy_commodities_summary_snapshot') }}
    {% if is_incremental() %}
    where snapshot_date >= COALESCE(
        (select max(snapshot_date) from {{ this }}),
        DATE '1900-01-01'
    ) - INTERVAL 1 MONTH
    {% endif %}
),

input_snapshot as (
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
        snapshot_date
    from {{ ref('input_commodities_summary_snapshot') }}
    {% if is_incremental() %}
    where snapshot_date >= COALESCE(
        (select max(snapshot_date) from {{ this }}),
        DATE '1900-01-01'
    ) - INTERVAL 1 MONTH
    {% endif %}
),

agriculture_snapshot as (
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
        snapshot_date
    from {{ ref('agriculture_commodities_summary_snapshot') }}
    {% if is_incremental() %}
    where snapshot_date >= COALESCE(
        (select max(snapshot_date) from {{ this }}),
        DATE '1900-01-01'
    ) - INTERVAL 1 MONTH
    {% endif %}
)

select * from energy_snapshot

union all

select * from input_snapshot

union all

select * from agriculture_snapshot
