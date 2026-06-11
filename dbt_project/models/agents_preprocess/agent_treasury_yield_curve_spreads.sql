{{
    config(
        materialized='incremental',
        unique_key='date',
        incremental_strategy='merge',
        tags=['agents_preprocess']
    )
}}

with pivoted_yields as (
    select
        safe_cast(date as date) as date,
        safe_cast(bc_1month as float64) as yield_1m,
        bc_3month as yield_3m,
        bc_6month as yield_6m,
        bc_1year as yield_1y,
        bc_2year as yield_2y,
        bc_3year as yield_3y,
        bc_5year as yield_5y,
        bc_7year as yield_7y,
        bc_10year as yield_10y,
        safe_cast(bc_20year as float64) as yield_20y,
        bc_30year as yield_30y
    from {{ ref('stg_treasury_yields') }}
    where date is not null
    {% if is_incremental() %}
    and safe_cast(date as date) >= COALESCE(
        (select max(date) from {{ this }}),
        DATE '1900-01-01'
    ) - INTERVAL 7 DAY
    {% endif %}
),

yield_spreads as (
    select
        date,
        yield_1m,
        yield_3m,
        yield_6m,
        yield_1y,
        yield_2y,
        yield_3y,
        yield_5y,
        yield_7y,
        yield_10y,
        yield_20y,
        yield_30y,
        yield_10y - yield_2y as spread_10y_2y,
        yield_10y - yield_3m as spread_10y_3m,
        yield_2y - yield_3m as spread_2y_3m,
        yield_30y - yield_2y as spread_30y_2y,
        case
            when yield_10y - yield_2y > 0.5 then 'Steep'
            when yield_10y - yield_2y > 0 then 'Normal'
            when yield_10y - yield_2y > -0.5 then 'Flat'
            else 'Inverted'
        end as curve_shape,
        case
            when yield_10y - yield_2y < 0 then 'Inverted'
            when yield_10y - yield_3m < 0 then 'Inverted (10Y-3M)'
            else 'Normal'
        end as inversion_status
    from pivoted_yields
)

select * from yield_spreads
