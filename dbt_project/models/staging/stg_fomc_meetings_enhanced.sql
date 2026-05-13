{{
    config(
        tags=['staging', 'fomc', 'calendar']
    )
}}

with source as (
    select * from {{ source('staging', 'fomc_meetings_enhanced') }}
),

cleaned as (
    select
        meeting_date,
        action,
        rate_change_bps,
        target_rate_lower,
        target_rate_upper,
        forecast_update,
        statement_url,
        transcript_available,
        transcript_release_date,
        summary_available,
        meeting_type,
        created_at,
        -- Extract year and quarter
        extract(year from meeting_date) as year,
        extract(quarter from meeting_date) as quarter,
        extract(month from meeting_date) as month,
        -- Calculate target rate midpoint
        (target_rate_lower + target_rate_upper) / 2.0 as target_rate_midpoint,
        -- Format rate change as percentage
        rate_change_bps / 100.0 as rate_change_pct,
        -- Flag for upcoming meetings
        coalesce(meeting_date > current_date, false) as is_upcoming,
        -- Days until/since meeting
        date_diff('day', current_date, meeting_date) as days_until_meeting
    from source
    where meeting_date is not null
)

select * from cleaned
