{{
    config(
        tags=['agents_preprocess']
    )
}}

select
    title,
    score,
    num_comments,
    subreddit,
    author,
    url,
    partition_date
from {{ source('staging', 'reddit_posts_raw') }}
