{{
    config(
        tags=['staging', 'fomc', 'sentiment']
    )
}}

with source as (
    select * from {{ source('staging', 'fomc_sentiment_scores') }}
),

cleaned as (
    select
        score_id,
        transcript_id,
        section_id,
        meeting_date,
        speaker,
        scoring_method,
        hawkish_score,
        dovish_score,
        net_sentiment_score,
        confidence,
        keyword_counts,
        total_hawkish_keywords,
        total_dovish_keywords,
        key_phrases,
        prev_meeting_score,
        score_delta,
        reasoning,
        model_name,
        created_at,
        extract(year from meeting_date) as year,
        extract(quarter from meeting_date) as quarter,
        -- Categorical label derived from net score
        case
            when net_sentiment_score > 0.1 then 'hawkish'
            when net_sentiment_score < -0.1 then 'dovish'
            else 'neutral'
        end as sentiment_label,
        -- Flag meeting-level aggregates
        section_id is null as is_meeting_aggregate
    from source
    where transcript_id is not null
)

select * from cleaned
