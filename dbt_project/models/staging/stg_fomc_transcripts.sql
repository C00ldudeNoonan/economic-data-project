{{
    config(
        tags=['staging', 'fomc', 'transcripts']
    )
}}

with source as (
    select * from {{ source('staging', 'fomc_transcripts') }}
),

typed as (
    select
        *,
        safe_cast(meeting_date as date) as meeting_dt
    from source
    where meeting_date is not null
),

cleaned as (
    select
        transcript_id,
        meeting_dt as meeting_date,
        full_text,
        word_count,
        page_count,
        source_url,
        source_pdf_path,
        processed_date,
        created_at,
        -- Extract year and quarter for partitioning/filtering
        extract(year from meeting_dt) as year,
        extract(quarter from meeting_dt) as quarter,
        extract(month from meeting_dt) as month,
        -- Calculate transcript age (years since released)
        date_diff(current_date(), meeting_dt, year) as years_since_meeting
    from typed
)

select * from cleaned
