{{
    config(
        materialized='view',
        tags=['staging', 'reddit', 'sentiment']
    )
}}

with source as (
    select * from {{ source('staging', 'reddit_sentiment_scored') }}
),

cleaned as (
    select
        content_id,
        content_type,
        lower(subreddit) as subreddit,
        partition_date,
        text_preview,
        compound as compound_score,
        positive as positive_score,
        negative as negative_score,
        neutral as neutral_score,
        label as sentiment_label,
        scored_at,
        -- Derived fields
        case
            when compound >= 0.5 then 'very_positive'
            when compound >= 0.05 then 'positive'
            when compound <= -0.5 then 'very_negative'
            when compound <= -0.05 then 'negative'
            else 'neutral'
        end as sentiment_strength,
        abs(compound) as sentiment_intensity
    from source
    where content_id is not null
      and content_type is not null
)

select * from cleaned
