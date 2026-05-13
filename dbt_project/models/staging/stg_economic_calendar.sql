{{
    config(
        tags=['staging', 'events', 'calendar']
    )
}}

with source as (
    select * from {{ source('staging', 'economic_calendar') }}
),

cleaned as (
    select
        event_id,
        title,
        country,
        -- Parse event datetime
        cast(try_cast(event_date as timestamp with time zone) as date) as event_date,
        impact,
        forecast,
        forecast_numeric,
        previous,
        previous_numeric,
        event_type,
        source,
        feed,
        fetched_at,
        try_cast(event_date as timestamp with time zone) as event_datetime,
        -- Extract time components
        extract(year from try_cast(event_date as timestamp with time zone)) as year,
        extract(month from try_cast(event_date as timestamp with time zone)) as month,
        extract(week from try_cast(event_date as timestamp with time zone)) as week_of_year,
        extract(dow from try_cast(event_date as timestamp with time zone)) as day_of_week,
        extract(hour from try_cast(event_date as timestamp with time zone)) as hour,
        -- Impact level as numeric for sorting
        case impact
            when 'High' then 3
            when 'Medium' then 2
            when 'Low' then 1
            when 'Holiday' then 0
            else -1
        end as impact_level,
        -- Flag for upcoming events
        coalesce(try_cast(event_date as timestamp with time zone) > current_timestamp, false) as is_upcoming,
        -- Days until event
        date_diff('day', current_date, cast(try_cast(event_date as timestamp with time zone) as date))
            as days_until_event
    from source
    where event_date is not null
)

select * from cleaned
