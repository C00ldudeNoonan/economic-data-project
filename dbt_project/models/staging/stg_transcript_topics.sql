{{
    config(
        enabled=false,
        tags=['staging', 'fomc', 'topics']
    )
}}

with source as (
    select * from {{ source('staging', 'transcript_topics') }}
),

cleaned as (
    select
        topic_id,
        transcript_id,
        section_id,
        topic,
        subtopic,
        relevance_score,
        mentioned_by,
        sentiment,
        created_at,
        -- Count speakers who mentioned this topic
        array_length(mentioned_by) as num_speakers
    from source
    where transcript_id is not null
)

select * from cleaned
