{{
    config(
        materialized='view',
        tags=['staging', 'reddit', 'tickers']
    )
}}

with source as (
    select * from {{ source('staging', 'reddit_ticker_mentions') }}
),

validated as (
    select
        t.ticker,
        t.content_id,
        t.content_type,
        lower(t.subreddit) as subreddit,
        t.partition_date,
        t.context_text,
        t.extracted_at,
        -- Enrich with company metadata when available
        c.company_name,
        c.sector,
        c.sub_industry,
        case when c.symbol is not null then true else false end as is_sp500
    from source t
    left join {{ ref('stg_sp500_companies_active') }} c
        on t.ticker = c.symbol
    where t.ticker is not null
      and t.content_id is not null
)

select * from validated
