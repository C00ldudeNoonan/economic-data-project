{{
    config(
        tags=['staging', 'fomc', 'metadata']
    )
}}

with source as (
    select * from {{ source('staging', 'fomc_minutes_metadata') }}
),

cleaned as (
    select
        meeting_date,
        year,
        title,
        gcs_path,
        gcs_uri,
        source_url,
        fetched_at,
        num_sections,
        content_length,
        -- Extract quarter from meeting date
        extract(quarter from meeting_date) as quarter,
        -- Extract month
        extract(month from meeting_date) as month
    from source
    where year >= 2010
)

select * from cleaned
