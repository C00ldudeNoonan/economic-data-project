with page_views as (
    select
        session_id,
        event_timestamp,
        page_path,
        row_number() over (partition by session_id order by event_timestamp) as step_number
    from {{ ref('stg_telemetry_events') }}
    where is_navigation_event and page_path is not null
),

page_transitions as (
    select
        pv1.session_id,
        pv1.page_path as current_page,
        pv2.page_path as next_page,
        pv1.step_number,
        timestamp_diff(pv2.event_timestamp, pv1.event_timestamp, second)
            as time_to_next_seconds
    from page_views as pv1
    left join page_views as pv2
        on
            pv1.session_id = pv2.session_id
            and pv2.step_number = pv1.step_number + 1
),

transition_aggregates as (
    select
        current_page,
        next_page,
        count(*) as transition_count,
        count(distinct session_id) as unique_sessions,
        avg(time_to_next_seconds) as avg_time_to_next_seconds,
        approx_quantiles(time_to_next_seconds, 100)[offset(50)]
            as median_time_to_next_seconds,

        avg(step_number) as avg_step_in_journey

    from page_transitions
    where next_page is not null
    group by current_page, next_page
),

page_metrics as (
    select
        page_path,
        count(*) as total_views,
        count(distinct session_id) as unique_sessions_viewing
    from page_views
    group by page_path
)

select
    ta.current_page,
    ta.next_page,
    ta.transition_count,
    ta.unique_sessions,

    pm_current.total_views as current_page_total_views,
    pm_next.total_views as next_page_total_views,
    round(ta.avg_time_to_next_seconds, 2) as avg_time_to_next_seconds,

    round(ta.median_time_to_next_seconds, 2) as median_time_to_next_seconds,

    round(ta.avg_step_in_journey, 1) as avg_step_in_journey,
    round(
        cast(ta.transition_count as float64) / nullif(pm_current.total_views, 0) * 100,
        2
    ) as transition_rate_pct,

    case
        when cast(ta.transition_count as float64)
            / nullif(pm_current.total_views, 0) * 100 < 20 then 'high_dropoff'
        when cast(ta.transition_count as float64)
            / nullif(pm_current.total_views, 0) * 100 < 50 then 'medium_dropoff'
        else 'low_dropoff'
    end as dropoff_category

from transition_aggregates as ta
left join page_metrics as pm_current
    on ta.current_page = pm_current.page_path
left join page_metrics as pm_next
    on ta.next_page = pm_next.page_path

where ta.transition_count >= 10

order by ta.transition_count desc
