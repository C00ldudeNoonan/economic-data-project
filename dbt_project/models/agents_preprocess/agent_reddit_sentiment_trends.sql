{{
    config(
        tags=['agents_preprocess']
    )
}}

select
    partition_date as date,
    subreddit,
    avg_score,
    median_score,
    num_posts as total_posts,
    weekly_avg_score,
    score_momentum_pct,
    avg_compound as avg_vader_sentiment,
    avg_post_sentiment,
    avg_comment_sentiment,
    pct_positive,
    pct_negative,
    sentiment_momentum,
    sentiment_trend
from {{ ref('reddit_sentiment_trends') }}
