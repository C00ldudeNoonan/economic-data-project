{{ config(
    tags=['data_quality']
) }}

-- Check 1: Z-Score from Rolling 21-Day Window
-- Flags prices > 2 standard deviations from their rolling 21-day mean.
-- Excludes the current row from the window to avoid self-reference.

{% set tables = ohlc_source_tables() %}

{% for table_name in tables %}
(
with {{ table_name }}_rolling as (
    select
        symbol,
        cast(date as date) as date,
        close,
        open,
        high,
        low,
        avg(close) over w as rolling_avg_close,
        stddev(close) over w as rolling_std_close,
        avg(open) over w as rolling_avg_open,
        stddev(open) over w as rolling_std_open
    from {{ source('staging', table_name) }}
    window w as (
        partition by symbol
        order by date
        rows between 21 preceding and 1 preceding
    )
)
select
    '{{ table_name }}' as source_table,
    symbol,
    date,
    'zscore' as check_type,
    coalesce(
        case
            when abs(close - rolling_avg_close) / nullif(rolling_std_close, 0) > abs(open - rolling_avg_open) / nullif(rolling_std_open, 0)
                then 'close zscore=' || round(abs(close - rolling_avg_close) / nullif(rolling_std_close, 0), 2)
            else 'open zscore=' || round(abs(open - rolling_avg_open) / nullif(rolling_std_open, 0), 2)
        end,
        'zscore anomaly detected'
    ) as failure_reason,
    open,
    high,
    low,
    close,
    CAST(NULL AS FLOAT64) as adj_close,
    current_timestamp as detected_at
from {{ table_name }}_rolling
where
    rolling_std_close is not null
    and rolling_std_close > 0
    and (
        abs(close - rolling_avg_close) / nullif(rolling_std_close, 0) > 2
        or abs(open - rolling_avg_open) / nullif(rolling_std_open, 0) > 2
    )
    -- Exclude dates within 2 days of a known stock split
    and not exists (
        select 1
        from {{ ref('corporate_actions') }} ca
        where ca.source_table = '{{ table_name }}'
          and ca.symbol = {{ table_name }}_rolling.symbol
          and ca.action_type = 'split'
          and {{ table_name }}_rolling.date between ca.date - interval '2' day and ca.date + interval '2' day
    )
)
{% if not loop.last %}union all{% endif %}
{% endfor %}
