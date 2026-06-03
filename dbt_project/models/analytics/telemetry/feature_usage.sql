with events as (
    select * from {{ ref('stg_telemetry_events') }}
    where is_feature_usage_event
),

daily_feature_usage as (
    select
        event_date as date,
        event_type,
        coalesce(feature_name, 'unspecified') as feature,

        count(*) as event_count,
        count(distinct session_id) as unique_sessions,
        count(distinct if(user_id is not null, user_id, null)) as unique_users,

        count(distinct if(chart_type is not null, chart_type, null))
            as unique_chart_types_used,
        countif(export_format is not null) as exports_with_format,

        avg(if(query_text is not null, length(query_text), null)) as avg_query_length,
        countif(query_text is not null) as queries_with_text

    from events
    group by event_date, event_type, feature
)

select
    date,
    event_type,
    feature,
    event_count,
    unique_sessions,
    unique_users,

    unique_chart_types_used,
    exports_with_format,

    queries_with_text,
    round(cast(event_count as float64) / nullif(unique_sessions, 0), 2)
        as events_per_session,

    round(cast(event_count as float64) / nullif(unique_users, 0), 2)
        as events_per_user,
    round(avg_query_length, 0) as avg_query_length_chars,

    round(
        cast(unique_sessions as float64)
        / nullif((
            select count(distinct session_id) from {{ ref('stg_sessions') }}
            where session_date = daily_feature_usage.date
        ), 0)
        * 100,
        2
    ) as session_penetration_pct

from daily_feature_usage
order by date desc, event_count desc
