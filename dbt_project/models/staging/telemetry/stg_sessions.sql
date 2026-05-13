{{
    config(
        materialized='table'
    )
}}

with session_events as (
    select
        session_id,
        user_id,
        event_type,
        event_timestamp,
        event_date,
        is_error_event,
        is_auth_event,
        is_navigation_event,
        is_feature_usage_event,
        page_url,
        page_path
    from {{ ref('stg_telemetry_events') }}
),

session_aggregates as (
    select
        session_id,

        -- User attribution
        max(user_id) as user_id,  -- Assumes user doesn't change mid-session
        count(distinct user_id) filter (where user_id is not null) as unique_users_in_session,

        -- Time boundaries
        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,
        date_trunc('day', min(event_timestamp)) as session_date,
        date_trunc('hour', min(event_timestamp)) as session_hour,

        -- Event counts
        count(*) as total_events,
        count(*) filter (where is_navigation_event) as page_views,
        count(*) filter (where is_feature_usage_event) as feature_interactions,
        count(*) filter (where is_auth_event) as auth_events,
        count(*) filter (where is_error_event) as error_count,

        -- Unique pages
        count(distinct page_path) filter (where page_path is not null) as unique_pages_visited,

        -- First and last pages
        (array_agg(page_path order by event_timestamp asc) filter (where page_path is not null))[1] as landing_page,
        (array_agg(page_path order by event_timestamp desc) filter (where page_path is not null))[1] as exit_page,

        -- Event type distribution
        count(*) filter (where event_type = 'page_view') as page_view_count,
        count(*) filter (where event_type = 'chat_query') as chat_query_count,
        count(*) filter (where event_type = 'chart_export') as chart_export_count,
        count(*) filter (where event_type = 'filter_applied') as filter_applied_count,
        count(*) filter (where event_type = 'sign_in') as sign_in_count,
        count(*) filter (where event_type = 'sign_up') as sign_up_count,
        count(*) filter (where event_type = 'sign_out') as sign_out_count,
        count(*) filter (where event_type = 'api_error') as api_error_count,
        count(*) filter (where event_type = 'app_error') as app_error_count,
        count(*) filter (where event_type = 'web_vitals') as web_vitals_count

    from session_events
    group by session_id
),

final as (
    select
        session_id,
        user_id,
        unique_users_in_session,

        -- Timestamps
        session_start,
        session_end,
        session_date,
        session_hour,

        -- Calculate duration in seconds
        total_events,

        -- Calculate duration in minutes (for easier interpretation)
        page_views,

        -- Event counts
        feature_interactions,
        auth_events,
        error_count,
        unique_pages_visited,
        landing_page,
        exit_page,

        -- Navigation
        page_view_count,
        chat_query_count,

        -- Event type details
        chart_export_count,
        filter_applied_count,
        sign_in_count,
        sign_up_count,
        sign_out_count,
        api_error_count,
        app_error_count,
        web_vitals_count,
        extract(epoch from (session_end - session_start)) as session_duration_seconds,
        round(extract(epoch from (session_end - session_start)) / 60.0, 2) as session_duration_minutes,

        -- Derived flags
        coalesce(user_id is not null, false) as is_authenticated,
        coalesce(page_views = 1, false) as is_bounce,
        coalesce(error_count > 0, false) as has_errors,
        coalesce(chat_query_count > 0, false) as used_chat,
        coalesce(chart_export_count > 0, false) as exported_chart,
        coalesce(sign_up_count > 0, false) as is_signup_session,
        coalesce(sign_in_count > 0, false) as is_signin_session,

        -- Engagement score (simple heuristic: more diverse interactions = higher engagement)
        (
            (case when page_views > 0 then 1 else 0 end)
            + (case when chat_query_count > 0 then 2 else 0 end)
            + (case when chart_export_count > 0 then 2 else 0 end)
            + (case when filter_applied_count > 0 then 1 else 0 end)
            + (case when unique_pages_visited > 3 then 1 else 0 end)
        ) as engagement_score

    from session_aggregates
)

select * from final
