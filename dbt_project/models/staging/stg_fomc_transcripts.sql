{{
    config(
        tags=['staging', 'fomc', 'transcripts']
    )
}}

with source as (
    select * from {{ source('staging', 'fomc_transcripts') }}
),

cleaned as (
    select
        transcript_id,
        meeting_date,
        full_text,
        word_count,
        page_count,
        source_url,
        source_pdf_path,
        processed_date,
        created_at,
        -- Extract year and quarter for partitioning/filtering
        extract(year from meeting_date) as year,
        extract(quarter from meeting_date) as quarter,
        extract(month from meeting_date) as month,
        -- Calculate transcript age (years since released)
        date_diff('year', meeting_date, current_date) as years_since_meeting
    from source
    where meeting_date is not null
)

select * from cleaned
