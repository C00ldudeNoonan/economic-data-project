{{
    config(
        tags=['analysis', 'reddit', 'threads']
    )
}}

-- Thread-level metrics computed from comment structure
-- Uses parent_id and depth to identify reply chains and controversial threads

with comments as (
    select
        comment_id,
        post_id,
        subreddit,
        partition_date,
        score,
        body_length,
        depth,
        is_top_level,
        score_tier
    from {{ ref('stg_reddit_comments') }}
),

thread_metrics as (
    select
        post_id,
        subreddit,
        partition_date,
        count(*) as total_comments,
        sum(case when is_top_level then 1 else 0 end) as top_level_comments,
        count(*) - sum(case when is_top_level then 1 else 0 end) as reply_comments,
        max(depth) as max_depth,
        avg(depth) as avg_depth,
        avg(score) as avg_comment_score,
        sum(case when score < 0 then 1 else 0 end) as negative_score_comments,
        avg(body_length) as avg_comment_length,
        max(body_length) as max_comment_length,
        -- Engagement distribution
        sum(case when score_tier = 'high' then 1 else 0 end) as high_score_comments,
        sum(case when score_tier = 'negative' then 1 else 0 end) as downvoted_comments
    from comments
    group by post_id, subreddit, partition_date
),

with_signals as (
    select
        t.*,
        p.title,
        p.score as post_score,
        p.num_comments as reported_comments,
        p.engagement_score,
        -- Thread structure signals
        case
            when max_depth >= 5 and total_comments >= 10 then true
            else false
        end as is_deep_thread,
        -- Controversy: high depth + mixed scores (both upvoted and downvoted)
        case
            when CAST(negative_score_comments AS FLOAT64) / nullif(total_comments, 0) >= 0.2
                and total_comments >= 5
            then true
            else false
        end as is_controversial,
        -- Reply ratio: how much discussion vs. standalone comments
        case
            when top_level_comments > 0
                then CAST(reply_comments AS FLOAT64) / top_level_comments
            else 0
        end as reply_ratio,
        -- Discussion quality: longer comments + deeper threads = more substantive
        case
            when avg_comment_length >= 200 and avg_depth >= 1.5 then 'high'
            when avg_comment_length >= 100 or avg_depth >= 1.0 then 'medium'
            else 'low'
        end as discussion_quality
    from thread_metrics t
    left join {{ ref('stg_reddit_posts') }} p
        on t.post_id = p.post_id
)

select * from with_signals
order by partition_date desc, total_comments desc
