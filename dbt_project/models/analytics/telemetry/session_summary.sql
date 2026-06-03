with sessions as (
    select * from {{ ref('stg_sessions') }}
),

daily_summary as (
    select
        session_date as date,

        count(distinct session_id) as total_sessions,
        count(distinct if(user_id is not null, user_id, null))
            as unique_authenticated_users,
        count(distinct if(is_authenticated, session_id, null))
            as authenticated_sessions,
        count(distinct if(not is_authenticated, session_id, null))
            as anonymous_sessions,

        avg(session_duration_seconds) as avg_duration_seconds,
        avg(session_duration_minutes) as avg_duration_minutes,
        approx_quantiles(session_duration_seconds, 100)[offset(50)]
            as median_duration_seconds,
        approx_quantiles(session_duration_seconds, 100)[offset(75)]
            as p75_duration_seconds,
        approx_quantiles(session_duration_seconds, 100)[offset(95)]
            as p95_duration_seconds,

        avg(total_events) as avg_events_per_session,
        avg(page_views) as avg_page_views_per_session,
        avg(feature_interactions) as avg_feature_interactions_per_session,
        avg(unique_pages_visited) as avg_unique_pages_per_session,
        avg(engagement_score) as avg_engagement_score,

        countif(is_bounce) / cast(nullif(count(*), 0) as float64) as bounce_rate,
        countif(has_errors) / cast(nullif(count(*), 0) as float64) as error_rate,
        countif(used_chat) / cast(nullif(count(*), 0) as float64) as chat_usage_rate,
        countif(exported_chart) / cast(nullif(count(*), 0) as float64) as export_rate,
        countif(is_authenticated) / cast(nullif(count(*), 0) as float64)
            as authenticated_session_rate,

        countif(is_signup_session) as signup_sessions,
        countif(is_signin_session) as signin_sessions,

        sum(page_view_count) as total_page_views,
        sum(chat_query_count) as total_chat_queries,
        sum(chart_export_count) as total_chart_exports,
        sum(filter_applied_count) as total_filter_applications,
        sum(api_error_count) as total_api_errors,
        sum(app_error_count) as total_app_errors,

        countif(engagement_score >= 5) as high_engagement_sessions,
        countif(engagement_score >= 3 and engagement_score < 5)
            as medium_engagement_sessions,
        countif(engagement_score < 3) as low_engagement_sessions

    from sessions
    group by session_date
)

select
    date,

    total_sessions,
    unique_authenticated_users,
    authenticated_sessions,
    anonymous_sessions,

    signup_sessions,
    signin_sessions,
    total_page_views,
    total_chat_queries,
    total_chart_exports,
    total_filter_applications,

    total_api_errors,
    total_app_errors,
    high_engagement_sessions,
    medium_engagement_sessions,
    low_engagement_sessions,

    case
        when unique_authenticated_users > 0
            then cast(total_sessions as float64) / unique_authenticated_users
    end as avg_sessions_per_user,
    round(avg_duration_seconds, 2) as avg_duration_seconds,
    round(avg_duration_minutes, 2) as avg_duration_minutes,
    round(median_duration_seconds, 2) as median_duration_seconds,
    round(p75_duration_seconds, 2) as p75_duration_seconds,

    round(p95_duration_seconds, 2) as p95_duration_seconds,
    round(avg_events_per_session, 2) as avg_events_per_session,

    round(avg_page_views_per_session, 2) as avg_page_views_per_session,
    round(avg_feature_interactions_per_session, 2)
        as avg_feature_interactions_per_session,
    round(avg_unique_pages_per_session, 2) as avg_unique_pages_per_session,
    round(avg_engagement_score, 2) as avg_engagement_score,
    round(bounce_rate * 100, 2) as bounce_rate_pct,
    round(error_rate * 100, 2) as error_rate_pct,

    round(chat_usage_rate * 100, 2) as chat_usage_rate_pct,
    round(export_rate * 100, 2) as export_rate_pct,
    round(authenticated_session_rate * 100, 2) as authenticated_session_rate_pct,

    round(
        cast(high_engagement_sessions as float64) / nullif(total_sessions, 0) * 100,
        2
    ) as high_engagement_pct,
    round(
        cast(medium_engagement_sessions as float64) / nullif(total_sessions, 0) * 100,
        2
    ) as medium_engagement_pct,
    round(
        cast(low_engagement_sessions as float64) / nullif(total_sessions, 0) * 100,
        2
    ) as low_engagement_pct

from daily_summary
order by date desc
