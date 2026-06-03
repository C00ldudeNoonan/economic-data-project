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

        max(user_id) as user_id,
        count(distinct if(user_id is not null, user_id, null))
            as unique_users_in_session,

        min(event_timestamp) as session_start,
        max(event_timestamp) as session_end,
        date_trunc(min(event_timestamp), day) as session_date,
        date_trunc(min(event_timestamp), hour) as session_hour,

        count(*) as total_events,
        countif(is_navigation_event) as page_views,
        countif(is_feature_usage_event) as feature_interactions,
        countif(is_auth_event) as auth_events,
        countif(is_error_event) as error_count,

        count(distinct if(page_path is not null, page_path, null))
            as unique_pages_visited,

        -- SAFE_OFFSET(0) = first element; array ordered asc gives landing page
        (array_agg(if(page_path is not null, page_path, null) ignore nulls
            order by event_timestamp asc))[safe_offset(0)] as landing_page,
        (array_agg(if(page_path is not null, page_path, null) ignore nulls
            order by event_timestamp desc))[safe_offset(0)] as exit_page,

        countif(event_type = 'page_view') as page_view_count,
        countif(event_type = 'chat_query') as chat_query_count,
        countif(event_type = 'chart_export') as chart_export_count,
        countif(event_type = 'filter_applied') as filter_applied_count,
        countif(event_type = 'sign_in') as sign_in_count,
        countif(event_type = 'sign_up') as sign_up_count,
        countif(event_type = 'sign_out') as sign_out_count,
        countif(event_type = 'api_error') as api_error_count,
        countif(event_type = 'app_error') as app_error_count,
        countif(event_type = 'web_vitals') as web_vitals_count

    from session_events
    group by session_id
),

final as (
    select
        session_id,
        user_id,
        unique_users_in_session,

        session_start,
        session_end,
        session_date,
        session_hour,

        total_events,
        page_views,
        feature_interactions,
        auth_events,
        error_count,
        unique_pages_visited,
        landing_page,
        exit_page,

        page_view_count,
        chat_query_count,
        chart_export_count,
        filter_applied_count,
        sign_in_count,
        sign_up_count,
        sign_out_count,
        api_error_count,
        app_error_count,
        web_vitals_count,
        timestamp_diff(session_end, session_start, second)
            as session_duration_seconds,
        round(
            cast(timestamp_diff(session_end, session_start, second) as float64)
            / 60.0,
            2
        ) as session_duration_minutes,

        coalesce(user_id is not null, false) as is_authenticated,
        coalesce(page_views = 1, false) as is_bounce,
        coalesce(error_count > 0, false) as has_errors,
        coalesce(chat_query_count > 0, false) as used_chat,
        coalesce(chart_export_count > 0, false) as exported_chart,
        coalesce(sign_up_count > 0, false) as is_signup_session,
        coalesce(sign_in_count > 0, false) as is_signin_session,

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
