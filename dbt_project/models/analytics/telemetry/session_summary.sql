with sessions as (
    select * from {{ ref('stg_sessions') }}
),

daily_summary as (
    select
        session_date as date,

        -- Session counts
        count(distinct session_id) as total_sessions,
        count(distinct user_id) filter (where user_id is not null) as unique_authenticated_users,
        count(distinct session_id) filter (where is_authenticated) as authenticated_sessions,
        count(distinct session_id) filter (where not is_authenticated) as anonymous_sessions,

        -- Duration metrics
        avg(session_duration_seconds) as avg_duration_seconds,
        avg(session_duration_minutes) as avg_duration_minutes,
        percentile_cont(0.5) within group (order by session_duration_seconds) as median_duration_seconds,
        percentile_cont(0.75) within group (order by session_duration_seconds) as p75_duration_seconds,
        percentile_cont(0.95) within group (order by session_duration_seconds) as p95_duration_seconds,

        -- Engagement metrics
        avg(total_events) as avg_events_per_session,
        avg(page_views) as avg_page_views_per_session,
        avg(feature_interactions) as avg_feature_interactions_per_session,
        avg(unique_pages_visited) as avg_unique_pages_per_session,
        avg(engagement_score) as avg_engagement_score,

        -- Behavior flags
        count(*) filter (where is_bounce)::float / nullif(count(*), 0)::float as bounce_rate,
        count(*) filter (where has_errors)::float / nullif(count(*), 0)::float as error_rate,
        count(*) filter (where used_chat)::float / nullif(count(*), 0)::float as chat_usage_rate,
        count(*) filter (where exported_chart)::float / nullif(count(*), 0)::float as export_rate,
        count(*) filter (where is_authenticated)::float / nullif(count(*), 0)::float as authenticated_session_rate,

        -- Conversion metrics
        count(*) filter (where is_signup_session) as signup_sessions,
        count(*) filter (where is_signin_session) as signin_sessions,

        -- Event type totals
        sum(page_view_count) as total_page_views,
        sum(chat_query_count) as total_chat_queries,
        sum(chart_export_count) as total_chart_exports,
        sum(filter_applied_count) as total_filter_applications,
        sum(api_error_count) as total_api_errors,
        sum(app_error_count) as total_app_errors,

        -- Session quality metrics
        count(*) filter (where engagement_score >= 5) as high_engagement_sessions,
        count(*) filter (where engagement_score >= 3 and engagement_score < 5) as medium_engagement_sessions,
        count(*) filter (where engagement_score < 3) as low_engagement_sessions

    from sessions
    group by session_date
)

select
    date,

    -- Session counts
    total_sessions,
    unique_authenticated_users,
    authenticated_sessions,
    anonymous_sessions,

    -- Calculate sessions per user
    signup_sessions,

    -- Duration metrics
    signin_sessions,
    total_page_views,
    total_chat_queries,
    total_chart_exports,
    total_filter_applications,

    -- Engagement metrics
    total_api_errors,
    total_app_errors,
    high_engagement_sessions,
    medium_engagement_sessions,
    low_engagement_sessions,

    -- Behavior rates
    case
        when unique_authenticated_users > 0
            then total_sessions::float / unique_authenticated_users::float
    end as avg_sessions_per_user,
    round(avg_duration_seconds, 2) as avg_duration_seconds,
    round(avg_duration_minutes, 2) as avg_duration_minutes,
    round(median_duration_seconds, 2) as median_duration_seconds,
    round(p75_duration_seconds, 2) as p75_duration_seconds,

    -- Conversion
    round(p95_duration_seconds, 2) as p95_duration_seconds,
    round(avg_events_per_session, 2) as avg_events_per_session,

    -- Event totals
    round(avg_page_views_per_session, 2) as avg_page_views_per_session,
    round(avg_feature_interactions_per_session, 2) as avg_feature_interactions_per_session,
    round(avg_unique_pages_per_session, 2) as avg_unique_pages_per_session,
    round(avg_engagement_score, 2) as avg_engagement_score,
    round(bounce_rate * 100, 2) as bounce_rate_pct,
    round(error_rate * 100, 2) as error_rate_pct,

    -- Session quality distribution
    round(chat_usage_rate * 100, 2) as chat_usage_rate_pct,
    round(export_rate * 100, 2) as export_rate_pct,
    round(authenticated_session_rate * 100, 2) as authenticated_session_rate_pct,

    -- Calculate engagement distribution percentages
    round(high_engagement_sessions::float / nullif(total_sessions, 0)::float * 100, 2) as high_engagement_pct,
    round(medium_engagement_sessions::float / nullif(total_sessions, 0)::float * 100, 2) as medium_engagement_pct,
    round(low_engagement_sessions::float / nullif(total_sessions, 0)::float * 100, 2) as low_engagement_pct

from daily_summary
order by date desc
