with error_events as (
    select * from {{ ref('stg_telemetry_events') }}
    where is_error_event
),

daily_error_summary as (
    select
        event_date as date,
        event_type,
        coalesce(error_message, 'unknown_error') as error_message,
        coalesce(error_code, 'no_code') as error_code,
        coalesce(page_path, 'unknown_page') as page_path,

        -- Error counts
        count(*) as error_count,
        count(distinct session_id) as affected_sessions,
        count(distinct user_id) filter (where user_id is not null) as affected_users,

        -- First and last occurrence
        min(event_timestamp) as first_occurrence,
        max(event_timestamp) as last_occurrence

    from error_events
    group by
        event_date,
        event_type,
        error_message,
        error_code,
        page_path
),

session_context as (
    -- Get total sessions per day for error rate calculation
    select
        session_date as date,
        count(distinct session_id) as total_sessions
    from {{ ref('stg_sessions') }}
    group by session_date
)

select
    es.date,
    es.event_type,
    es.error_message,
    es.error_code,
    es.page_path,
    es.error_count,
    es.affected_sessions,
    es.affected_users,
    es.first_occurrence,
    es.last_occurrence,

    -- Calculate time span
    extract(epoch from (es.last_occurrence - es.first_occurrence)) / 3600.0 as hours_between_first_and_last,

    -- Calculate error rate
    round(
        es.affected_sessions::float / nullif(sc.total_sessions, 0)::float * 100,
        2
    ) as session_error_rate_pct,

    -- Calculate average errors per affected session
    round(es.error_count::float / nullif(es.affected_sessions, 0)::float, 2) as avg_errors_per_affected_session,

    -- Severity indicator
    case
        when es.error_count > 100 then 'critical'
        when es.error_count > 50 then 'high'
        when es.error_count > 10 then 'medium'
        else 'low'
    end as severity,

    -- Impact indicator (combination of frequency and user impact)
    case
        when es.error_count > 100 and es.affected_sessions > 20 then 'high_impact'
        when es.error_count > 50 or es.affected_sessions > 10 then 'medium_impact'
        else 'low_impact'
    end as impact_level

from daily_error_summary as es
left join session_context as sc
    on es.date = sc.date

order by es.date desc, es.error_count desc
