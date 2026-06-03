{{
    config(
        tags=['analysis', 'reddit', 'cross_subreddit']
    )
}}

-- Detect stories/topics discussed across multiple subreddits
-- Uses URL overlap and title similarity to find cross-posted content

with posts as (
    select
        post_id,
        title,
        url,
        domain,
        subreddit,
        partition_date,
        score,
        num_comments,
        engagement_score,
        is_self_post
    from {{ ref('stg_reddit_posts') }}
    where not is_deleted
),

-- Method 1: Same external URL shared across subreddits
url_overlap as (
    select
        url,
        domain,
        partition_date,
        count(distinct subreddit) as subreddit_count,
        array_agg(distinct subreddit order by subreddit) as subreddits,
        sum(score) as total_score,
        sum(num_comments) as total_comments,
        sum(engagement_score) as total_engagement,
        min(post_id) as first_post_id
    from posts
    where not is_self_post
      and url is not null
      and length(url) > 0
      and domain not in ('self.investing', 'self.stocks', 'self.wallstreetbets',
                          'self.economics', 'self.economy')
    group by url, domain, partition_date
    having count(distinct subreddit) >= 2
),

-- Method 2: Title similarity — exact title matches (case-insensitive)
title_overlap as (
    select
        lower(trim(title)) as normalized_title,
        partition_date,
        count(distinct subreddit) as subreddit_count,
        array_agg(distinct subreddit order by subreddit) as subreddits,
        sum(score) as total_score,
        sum(num_comments) as total_comments,
        sum(engagement_score) as total_engagement,
        min(post_id) as first_post_id
    from posts
    where length(title) > 20  -- skip very short titles
    group by lower(trim(title)), partition_date
    having count(distinct subreddit) >= 2
),

combined as (
    select
        'url_match' as match_type,
        url as match_key,
        domain as match_domain,
        partition_date,
        subreddit_count,
        subreddits,
        total_score,
        total_comments,
        total_engagement,
        first_post_id
    from url_overlap

    union all

    select
        'title_match' as match_type,
        normalized_title as match_key,
        null as match_domain,
        partition_date,
        subreddit_count,
        subreddits,
        total_score,
        total_comments,
        total_engagement,
        first_post_id
    from title_overlap
),

with_context as (
    select
        c.*,
        p.title as post_title,
        -- Flag high-attention cross-posts
        case
            when subreddit_count >= 3 then 'viral'
            when total_engagement >= 500 then 'high_attention'
            else 'normal'
        end as attention_level,
        -- Track which subreddit combinations are common
        case
            when 'wallstreetbets' in unnest(subreddits)
                and ('economics' in unnest(subreddits)
                     or 'economy' in unnest(subreddits))
            then true
            else false
        end as retail_to_academic_crossover
    from combined c
    left join {{ ref('stg_reddit_posts') }} p
        on c.first_post_id = p.post_id
)

select * from with_context
order by partition_date desc, total_engagement desc
