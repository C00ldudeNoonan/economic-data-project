{{
    config(
        materialized='incremental',
        unique_key='date',
        incremental_strategy='delete+insert',
        tags=['agents_preprocess']
    )
}}

with pivoted_yields as (
    select
        date,
        max(case when yield_type = 'BC_1MONTH' then value end) as yield_1m,
        max(case when yield_type = 'BC_3MONTH' then value end) as yield_3m,
        max(case when yield_type = 'BC_6MONTH' then value end) as yield_6m,
        max(case when yield_type = 'BC_1YEAR' then value end) as yield_1y,
        max(case when yield_type = 'BC_2YEAR' then value end) as yield_2y,
        max(case when yield_type = 'BC_3YEAR' then value end) as yield_3y,
        max(case when yield_type = 'BC_5YEAR' then value end) as yield_5y,
        max(case when yield_type = 'BC_7YEAR' then value end) as yield_7y,
        max(case when yield_type = 'BC_10YEAR' then value end) as yield_10y,
        max(case when yield_type = 'BC_20YEAR' then value end) as yield_20y,
        max(case when yield_type = 'BC_30YEAR' then value end) as yield_30y
    from {{ ref('stg_treasury_yields') }}
    where date is not null
    {% if is_incremental() %}
    and date >= COALESCE(
        (select max(date) from {{ this }}),
        DATE '1900-01-01'
    ) - INTERVAL 7 DAY
    {% endif %}
    group by date
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
