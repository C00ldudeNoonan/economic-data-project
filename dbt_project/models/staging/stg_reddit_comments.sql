{{
    config(
        tags=['staging', 'reddit', 'comments']
    )
}}

with source as (
    select * from {{ source('staging', 'reddit_comments_raw') }}
),

cleaned as (
    select
        comment_id,
        post_id,
        author,
        body,
        score,
        created_utc,
        parent_id,
        depth,
        links,
        lower(subreddit) as subreddit,
        partition_date,
        fetched_at,
        -- Derived fields
        length(body) as body_length,
        coalesce(links != '' and links is not null, false) as has_links,
        coalesce(author = '[deleted]', false) as is_deleted,
        -- Is this a top-level comment (reply to post, not another comment)?
        coalesce(starts_with(parent_id, 't3_'), false) as is_top_level,
        -- Engagement bucket
        case
            when score >= 50 then 'high'
            when score >= 10 then 'medium'
            when score >= 1 then 'low'
            else 'negative'
        end as score_tier,
        -- Time features
        extract(dayofweek from created_utc) as day_of_week,
        extract(hour from created_utc) as hour_of_day
    from source
    where
        body is not null
        and length(body) > 0
        and author != '[deleted]'
)

select * from cleaned
