{{ config(
    tags=['data_quality']
) }}

-- Check 4: Stale Price Detection
-- Catches days where the feed returned yesterday's exact data.

{% set tables = ohlc_source_tables() %}

{% for table_name in tables %}
(
with {{ table_name }}_consecutive as (
    select
        symbol,
        SAFE_CAST(SUBSTR(CAST(date AS STRING), 1, 10) AS DATE) AS date,
        close,
        open,
        high,
        low,
        lag(close) over (partition by symbol order by date) as prev_close,
        lag(open) over (partition by symbol order by date) as prev_open,
        lag(high) over (partition by symbol order by date) as prev_high,
        lag(low) over (partition by symbol order by date) as prev_low
    from {{ source('staging', table_name) }}
)
select
    '{{ table_name }}' as source_table,
    symbol,
    date,
    'stale_price' as check_type,
    'all OHLC identical to previous day' as failure_reason,
    open,
    high,
    low,
    close,
    CAST(NULL AS FLOAT64) as adj_close,
    current_timestamp as detected_at
from {{ table_name }}_consecutive
where
    close = prev_close
    and open = prev_open
    and high = prev_high
    and low = prev_low
    and prev_close is not null
)
{% if not loop.last %}union all{% endif %}
{% endfor %}
