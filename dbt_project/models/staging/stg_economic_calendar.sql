{{
    config(
        tags=['staging', 'events', 'calendar']
    )
}}

with source as (
    select * from {{ source('staging', 'economic_calendar') }}
),

typed as (
    select
        *,
        safe_cast(date as timestamp) as event_timestamp
    from source
    where date is not null
),

cleaned as (
    select
        coalesce(
            event_id,
            farm_fingerprint(concat(
                coalesce(cast(event_timestamp as string), ''),
                '|',
                coalesce(title, ''),
                '|',
                coalesce(country, '')
            ))
        ) as event_id,
        title,
        country,
        -- Parse event datetime
        cast(event_timestamp as date) as event_date,
        impact,
        forecast,
        forecast as forecast_numeric,
        previous,
        previous as previous_numeric,
        actual,
        event_type,
        source,
        fetched_at,
        event_timestamp as event_datetime,
        -- Extract time components
        extract(year from event_timestamp) as year,
        extract(month from event_timestamp) as month,
        extract(week from event_timestamp) as week_of_year,
        extract(dayofweek from event_timestamp) as day_of_week,
        extract(hour from event_timestamp) as hour,
        -- Impact level as numeric for sorting
        case impact
            when 'High' then 3
            when 'Medium' then 2
            when 'Low' then 1
            when 'Holiday' then 0
            else -1
        end as impact_level,
        -- Flag for upcoming events
        coalesce(event_timestamp > current_timestamp(), false) as is_upcoming,
        -- Days until event
        date_diff(cast(event_timestamp as date), current_date(), day)
            as days_until_event
    from typed
)

select * from cleaned
qualify row_number() over (
    partition by event_id
    order by fetched_at desc
) = 1
