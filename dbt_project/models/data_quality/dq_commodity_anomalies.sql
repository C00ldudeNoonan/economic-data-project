{{ config(
    tags=['data_quality']
) }}

-- Check 6: Commodity-specific data quality checks
-- Combines price validation, z-score anomalies, and day-over-day spikes
-- for commodity tables which have a different schema (commodity_price instead of OHLC).

{% set tables = commodity_source_tables() %}

{% for table_name in tables %}
(
with {{ table_name }}_enriched as (
    select
        commodity_name,
        cast(date as date) as date,
        CAST(commodity_price AS FLOAT64) as price,
        lag(CAST(commodity_price AS FLOAT64)) over (
            partition by commodity_name order by date
        ) as prev_price,
        avg(CAST(commodity_price AS FLOAT64)) over (
            partition by commodity_name
            order by date
            rows between 21 preceding and 1 preceding
        ) as rolling_avg,
        stddev(CAST(commodity_price AS FLOAT64)) over (
            partition by commodity_name
            order by date
            rows between 21 preceding and 1 preceding
        ) as rolling_std
    from {{ source('staging', table_name) }}
    where commodity_price is not null
      and date is not null
)
-- Price <= 0
select
    '{{ table_name }}' as source_table,
    commodity_name as symbol,
    date,
    'invalid_price' as check_type,
    coalesce('price=' || CAST(price AS STRING), 'invalid price') as failure_reason,
    CAST(NULL AS FLOAT64) as open,
    CAST(NULL AS FLOAT64) as high,
    CAST(NULL AS FLOAT64) as low,
    price as close,
    CAST(NULL AS FLOAT64) as adj_close,
    current_timestamp as detected_at
from {{ table_name }}_enriched
where price <= 0

union all

-- Z-score > 2
select
    '{{ table_name }}' as source_table,
    commodity_name as symbol,
    date,
    'zscore' as check_type,
    coalesce(
        'price zscore=' || round(abs(price - rolling_avg) / nullif(rolling_std, 0), 2),
        'zscore anomaly'
    ) as failure_reason,
    CAST(NULL AS FLOAT64) as open,
    CAST(NULL AS FLOAT64) as high,
    CAST(NULL AS FLOAT64) as low,
    price as close,
    CAST(NULL AS FLOAT64) as adj_close,
    current_timestamp as detected_at
from {{ table_name }}_enriched
where
    rolling_std is not null
    and rolling_std > 0
    and abs(price - rolling_avg) / nullif(rolling_std, 0) > 2

union all

-- Day-over-day return > 15%
select
    '{{ table_name }}' as source_table,
    commodity_name as symbol,
    date,
    'return_spike' as check_type,
    coalesce(
        'daily return ' || round((price / nullif(prev_price, 0) - 1) * 100, 1) || '%',
        'return spike'
    ) as failure_reason,
    CAST(NULL AS FLOAT64) as open,
    CAST(NULL AS FLOAT64) as high,
    CAST(NULL AS FLOAT64) as low,
    price as close,
    CAST(NULL AS FLOAT64) as adj_close,
    current_timestamp as detected_at
from {{ table_name }}_enriched
where
    prev_price is not null
    and prev_price > 0
    and abs(price / prev_price - 1) > 0.15

union all

-- Stale price (identical to previous day)
select
    '{{ table_name }}' as source_table,
    commodity_name as symbol,
    date,
    'stale_price' as check_type,
    'price identical to previous day' as failure_reason,
    CAST(NULL AS FLOAT64) as open,
    CAST(NULL AS FLOAT64) as high,
    CAST(NULL AS FLOAT64) as low,
    price as close,
    CAST(NULL AS FLOAT64) as adj_close,
    current_timestamp as detected_at
from {{ table_name }}_enriched
where
    prev_price is not null
    and price = prev_price
)
{% if not loop.last %}union all{% endif %}
{% endfor %}
