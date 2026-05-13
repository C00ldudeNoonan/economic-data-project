{{
    config(
        tags=['staging', 'reddit', 'content']
    )
}}

with source as (
    select * from {{ source('staging', 'reddit_post_content_raw') }}
),

posts as (
    select * from {{ ref('stg_reddit_posts') }}
),

cleaned as (
    select
        s.post_id,
        s.title,
        s.selftext,
        s.links,
        s.author,
        s.score,
        s.url,
        s.created_utc,
        lower(s.subreddit) as subreddit,
        s.partition_date,
        s.fetched_at,
        -- Derived fields
        length(s.selftext) as selftext_length,
        coalesce(length(s.selftext) > 0, false) as has_selftext,
        coalesce(s.links != '' and s.links is not null, false) as has_links,
        -- Link count (comma-separated links field)
        case
            when s.links = '' or s.links is null then 0
            else length(s.links) - length(replace(s.links, ',', '')) + 1
        end as link_count,
        -- Join enrichment from posts staging
        p.engagement_score,
        p.num_comments,
        p.is_deleted
    from source s
    left join posts p on s.post_id = p.post_id
    where
        -- Filter out ads that may have slipped through
        lower(s.subreddit) not like 'u\_%' escape '\'
)

select * from cleaned
