{{
    config(
        tags=['staging', 'fomc', 'transcript_sections']
    )
}}

with source as (
    select * from {{ source('staging', 'transcript_sections') }}
),

cleaned as (
    select
        section_id,
        transcript_id,
        section_order,
        section_type,
        speaker,
        speaker_role,
        content,
        start_page,
        end_page,
        created_at,
        -- Calculate content length
        length(content) as content_length,
        -- Extract word count (approximate)
        length(content) - length(replace(content, ' ', '')) + 1 as word_count
    from source
    where transcript_id is not null
)

select * from cleaned
