{{
    config(
        tags=['analysis', 'reddit', 'sentiment']
    )
}}

with daily_engagement as (
    select
        partition_date,
        subreddit,
        count(*) as num_posts,
        avg(score) as avg_score,
        avg(num_comments) as avg_comments,
        sum(num_comments) as total_comments,
        sum(engagement_score) as total_engagement,
        approx_percentile(score, 0.5) as median_score,
        approx_percentile(score, 0.75) as p75_score,
        approx_percentile(score, 0.90) as p90_score,
        max(score) as max_score,
        max(num_comments) as max_comments,
        avg(case when is_self_post then 1.0 else 0.0 end) as self_post_ratio,
        avg(case when is_deleted then 1.0 else 0.0 end) as deleted_post_ratio
    from {{ ref('stg_reddit_posts') }}
    group by partition_date, subreddit
),

daily_sentiment as (
    select
        partition_date,
        subreddit,
        count(*) as total_scored,
        avg(compound_score) as avg_compound,
        avg(case when content_type like 'post%' then compound_score end) as avg_post_sentiment,
        avg(case when content_type = 'comment' then compound_score end) as avg_comment_sentiment,
        sum(case when sentiment_label = 'positive' then 1 else 0 end) / 1.0
            / nullif(count(*), 0) * 100 as pct_positive,
        sum(case when sentiment_label = 'negative' then 1 else 0 end) / 1.0
            / nullif(count(*), 0) * 100 as pct_negative,
        avg(sentiment_intensity) as avg_intensity,
        sum(case when sentiment_strength = 'very_positive' then 1 else 0 end) as very_positive_count,
        sum(case when sentiment_strength = 'very_negative' then 1 else 0 end) as very_negative_count
    from {{ ref('stg_reddit_sentiment') }}
    group by partition_date, subreddit
),

combined as (
    select
        e.*,
        s.total_scored,
        s.avg_compound,
        s.avg_post_sentiment,
        s.avg_comment_sentiment,
        s.pct_positive,
        s.pct_negative,
        s.avg_intensity,
        s.very_positive_count,
        s.very_negative_count
    from daily_engagement e
    left join daily_sentiment s
        on e.partition_date = s.partition_date
        and e.subreddit = s.subreddit
),

with_rolling as (
    select
        *,
        -- 7-day rolling engagement averages
        avg(avg_score) over (
            partition by subreddit
            order by partition_date
            rows between 7 preceding and 1 preceding
        ) as weekly_avg_score,
        avg(avg_comments) over (
            partition by subreddit
            order by partition_date
            rows between 7 preceding and 1 preceding
        ) as weekly_avg_comments,
        avg(num_posts) over (
            partition by subreddit
            order by partition_date
            rows between 7 preceding and 1 preceding
        ) as weekly_avg_posts,
        -- 7-day rolling sentiment averages
        avg(avg_compound) over (
            partition by subreddit
            order by partition_date
            rows between 7 preceding and 1 preceding
        ) as weekly_avg_sentiment
    from combined
),

with_momentum as (
    select
        *,
        case
            when weekly_avg_score > 0
                then ((avg_score - weekly_avg_score) / weekly_avg_score) * 100
            else 0
        end as score_momentum_pct,
        case
            when weekly_avg_comments > 0
                then ((avg_comments - weekly_avg_comments) / weekly_avg_comments) * 100
            else 0
        end as comments_momentum_pct,
        case
            when weekly_avg_posts > 0
                then ((num_posts - weekly_avg_posts) / weekly_avg_posts) * 100
            else 0
        end as activity_momentum_pct,
        -- Sentiment momentum
        case
            when weekly_avg_sentiment is not null
                then avg_compound - weekly_avg_sentiment
            else null
        end as sentiment_momentum,
        -- Combined trend indicator (engagement + sentiment)
        case
            when avg_compound > 0.05 and avg_score > coalesce(weekly_avg_score, 0) then 'bullish'
            when avg_compound < -0.05 and avg_score < coalesce(weekly_avg_score, 0) then 'bearish'
            when avg_compound > 0.05 then 'positive'
            when avg_compound < -0.05 then 'negative'
            else 'neutral'
        end as sentiment_trend
    from with_rolling
)

select * from with_momentum
order by partition_date desc, subreddit asc
