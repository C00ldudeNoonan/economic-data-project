{{
    config(
        tags=['staging', 'fomc', 'metadata']
    )
}}

with source as (
    select * from {{ source('staging', 'fomc_minutes_metadata') }}
),

typed as (
    select
        *,
        safe_cast(meeting_date as date) as meeting_dt
    from source
),

cleaned as (
    select
        meeting_dt as meeting_date,
        year,
        title,
        gcs_path,
        gcs_uri,
        source_url,
        fetched_at,
        num_sections,
        content_length,
        -- Extract quarter from meeting date
        extract(quarter from meeting_dt) as quarter,
        -- Extract month
        extract(month from meeting_dt) as month
    from typed
    where year >= 2010
)

select * from cleaned
