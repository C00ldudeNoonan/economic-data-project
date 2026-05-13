with events as (
    select * from {{ ref('stg_telemetry_events') }}
    where is_feature_usage_event
),

daily_feature_usage as (
    select
        event_date as date,
        event_type,
        coalesce(feature_name, 'unspecified') as feature,

        -- Event counts
        count(*) as event_count,
        count(distinct session_id) as unique_sessions,
        count(distinct user_id) filter (where user_id is not null) as unique_users,

        -- Chart-specific metrics
        count(distinct chart_type) filter (where chart_type is not null) as unique_chart_types_used,
        count(*) filter (where export_format is not null) as exports_with_format,

        -- Query-specific metrics
        avg(length(query_text)) filter (where query_text is not null) as avg_query_length,
        count(*) filter (where query_text is not null) as queries_with_text

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

    -- Calculate usage intensity
    unique_chart_types_used,
    exports_with_format,

    -- Chart metrics
    queries_with_text,
    round(event_count::float / nullif(unique_sessions, 0)::float, 2) as events_per_session,

    -- Query metrics
    round(event_count::float / nullif(unique_users, 0)::float, 2) as events_per_user,
    round(avg_query_length, 0) as avg_query_length_chars,

    -- Calculate engagement rate (what % of sessions used this feature)
    round(
        unique_sessions::float
        / nullif((
            select count(distinct session_id) from {{ ref('stg_sessions') }}
            where session_date = daily_feature_usage.date
        ), 0)::float
        * 100,
        2
    ) as session_penetration_pct

from daily_feature_usage
order by date desc, event_count desc
