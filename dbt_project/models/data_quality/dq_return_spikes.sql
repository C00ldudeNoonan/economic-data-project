{{ config(
    tags=['data_quality']
) }}

-- Check 2: Day-over-Day Return Spike Detection
-- Flags single-day moves > 15% which are likely data errors for large-cap stocks/ETFs.

{% set tables = ohlc_source_tables() %}

{% for table_name in tables %}
(
with {{ table_name }}_returns as (
    select
        symbol,
        cast(date as date) as date,
        close,
        open,
        high,
        low,
        lag(close) over (partition by symbol order by date) as prev_close,
        (close / nullif(lag(close) over (partition by symbol order by date), 0) - 1) as daily_return,
        (open / nullif(lag(close) over (partition by symbol order by date), 0) - 1) as overnight_return
    from {{ source('staging', table_name) }}
)
select
    '{{ table_name }}' as source_table,
    symbol,
    date,
    'return_spike' as check_type,
    coalesce(
        case
            when abs(daily_return) > 0.15 and abs(overnight_return) > 0.15
                then 'daily return ' || round(daily_return * 100, 1) || '% and overnight ' || round(overnight_return * 100, 1) || '%'
            when abs(daily_return) > 0.15
                then 'daily return ' || round(daily_return * 100, 1) || '%'
            else 'overnight return ' || round(overnight_return * 100, 1) || '%'
        end,
        'return spike detected'
    ) as failure_reason,
    open,
    high,
    low,
    close,
    CAST(NULL AS FLOAT64) as adj_close,
    current_timestamp as detected_at
from {{ table_name }}_returns
where
    prev_close is not null
    and (abs(daily_return) > 0.15 or abs(overnight_return) > 0.15)
    -- Exclude known stock splits (these cause legitimate large price moves)
    and not exists (
        select 1
        from {{ ref('corporate_actions') }} ca
        where ca.source_table = '{{ table_name }}'
          and ca.symbol = {{ table_name }}_returns.symbol
          and ca.date = {{ table_name }}_returns.date
          and ca.action_type = 'split'
    )
)
{% if not loop.last %}union all{% endif %}
{% endfor %}
