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
        safe_cast(partition_date as date) as partition_date,
        fetched_at,
        -- Derive additional fields
        coalesce(domain like '%self.%', false) as is_self_post,
        coalesce(url like '%/r/%', false) or coalesce(domain like '%self.%', false) as is_text_post,
        -- Engagement metrics
        score + num_comments as engagement_score,
        case
            when num_comments > 0 then cast(score as float64) / cast(num_comments as float64)
            else cast(score as float64)
        end as score_per_comment,
        -- Time features
        extract(dayofweek from created_utc) as day_of_week,
        extract(hour from created_utc) as hour_of_day,
        -- Deleted/removed indicators
        coalesce(author = '[deleted]', false) as is_deleted
    from source
    where
        safe_cast(partition_date as date) >= date_sub(current_date(), interval 90 day)
        and score is not null
        and title is not null
        and length(title) > 0
        -- Filter out Reddit promoted/ad posts (u_* subreddits)
        and not starts_with(lower(subreddit), 'u_')
)

select * from cleaned
