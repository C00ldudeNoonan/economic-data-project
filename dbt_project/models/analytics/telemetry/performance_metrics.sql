with web_vitals_events as (
    select
        event_date,
        event_timestamp,
        page_path,
        session_id,
        user_id,
        metric_name,
        metric_value,
        metric_rating
    from {{ ref('stg_telemetry_events') }}
    where
        is_performance_event
        and metric_name is not null
        and metric_value is not null
),

daily_metrics as (
    select
        event_date as date,
        metric_name,
        coalesce(page_path, 'unknown_page') as page,

        -- Statistical aggregations
        count(*) as sample_size,
        avg(metric_value) as avg_value,
        percentile_cont(0.50) within group (order by metric_value) as p50_value,
        percentile_cont(0.75) within group (order by metric_value) as p75_value,
        percentile_cont(0.95) within group (order by metric_value) as p95_value,
        percentile_cont(0.99) within group (order by metric_value) as p99_value,
        min(metric_value) as min_value,
        max(metric_value) as max_value,

        -- Rating distribution
        count(*) filter (where metric_rating = 'good') as good_count,
        count(*) filter (where metric_rating = 'needs-improvement') as needs_improvement_count,
        count(*) filter (where metric_rating = 'poor') as poor_count,

        -- Unique sessions and users
        count(distinct session_id) as unique_sessions,
        count(distinct user_id) filter (where user_id is not null) as unique_users

    from web_vitals_events
    group by event_date, page, metric_name
)

select
    date,
    page,
    metric_name,
    sample_size,

    -- Central tendency metrics
    good_count,
    needs_improvement_count,
    poor_count,
    unique_sessions,
    unique_users,
    round(avg_value, 2) as avg_value,
    round(p50_value, 2) as p50_value,

    -- Rating distribution counts
    round(p75_value, 2) as p75_value,
    round(p95_value, 2) as p95_value,
    round(p99_value, 2) as p99_value,

    -- Rating distribution percentages
    round(min_value, 2) as min_value,
    round(max_value, 2) as max_value,
    round(good_count::float / nullif(sample_size, 0)::float * 100, 2) as good_pct,

    -- User reach
    round(needs_improvement_count::float / nullif(sample_size, 0)::float * 100, 2) as needs_improvement_pct,
    round(poor_count::float / nullif(sample_size, 0)::float * 100, 2) as poor_pct,

    -- Overall rating based on p75 value and Web Vitals thresholds
    case
        when metric_name = 'LCP'
            then
                case
                    when p75_value <= 2500 then 'good'
                    when p75_value <= 4000 then 'needs-improvement'
                    else 'poor'
                end
        when metric_name = 'FID'
            then
                case
                    when p75_value <= 100 then 'good'
                    when p75_value <= 300 then 'needs-improvement'
                    else 'poor'
                end
        when metric_name = 'CLS'
            then
                case
                    when p75_value <= 0.1 then 'good'
                    when p75_value <= 0.25 then 'needs-improvement'
                    else 'poor'
                end
        when metric_name = 'FCP'
            then
                case
                    when p75_value <= 1800 then 'good'
                    when p75_value <= 3000 then 'needs-improvement'
                    else 'poor'
                end
        when metric_name = 'TTFB'
            then
                case
                    when p75_value <= 800 then 'good'
                    when p75_value <= 1800 then 'needs-improvement'
                    else 'poor'
                end
        when metric_name = 'INP'
            then
                case
                    when p75_value <= 200 then 'good'
                    when p75_value <= 500 then 'needs-improvement'
                    else 'poor'
                end
        else 'unknown'
    end as overall_rating,

    -- Performance trend indicator (compare p95 vs avg)
    case
        when p95_value > (avg_value * 2) then 'high_variability'
        when p95_value > (avg_value * 1.5) then 'moderate_variability'
        else 'low_variability'
    end as variability_level

from daily_metrics
where sample_size >= 5  -- Filter out pages with too few samples

order by date desc, metric_name asc, page asc
