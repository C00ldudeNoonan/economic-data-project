{{
    config(
        tags=['staging', 'reddit', 'news']
    )
}}

with source as (
    select * from {{ source('staging', 'reddit_posts_raw') }}
),

cleaned as (
    select
        post_id,
        title,
        score,
        num_comments,
        created_utc,
        author,
        url,
        permalink,
        lower(subreddit) as subreddit,
        domain,
        partition_date,
        fetched_at,
        -- Derive additional fields
        coalesce(domain like '%self.%', false) as is_self_post,
        coalesce(url like '%/r/%', false) or coalesce(domain like '%self.%', false) as is_text_post,
        -- Engagement metrics
        score + num_comments as engagement_score,
        case
            when num_comments > 0 then cast(score as double) / cast(num_comments as double)
            else cast(score as double)
        end as score_per_comment,
        -- Time features
        extract(dayofweek from created_utc) as day_of_week,
        extract(hour from created_utc) as hour_of_day,
        -- Deleted/removed indicators
        coalesce(author = '[deleted]', false) as is_deleted
    from source
    where
        partition_date >= current_date - interval '90 days'
        and score is not null
        and title is not null
        and length(title) > 0
        -- Filter out Reddit promoted/ad posts (u_* subreddits)
        and lower(subreddit) not like 'u\_%' escape '\'
)

select * from cleaned
