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

        count(*) as sample_size,
        avg(metric_value) as avg_value,
        approx_quantiles(metric_value, 100)[offset(50)] as p50_value,
        approx_quantiles(metric_value, 100)[offset(75)] as p75_value,
        approx_quantiles(metric_value, 100)[offset(95)] as p95_value,
        approx_quantiles(metric_value, 100)[offset(99)] as p99_value,
        min(metric_value) as min_value,
        max(metric_value) as max_value,

        countif(metric_rating = 'good') as good_count,
        countif(metric_rating = 'needs-improvement') as needs_improvement_count,
        countif(metric_rating = 'poor') as poor_count,

        count(distinct session_id) as unique_sessions,
        count(distinct if(user_id is not null, user_id, null)) as unique_users

    from web_vitals_events
    group by event_date, page, metric_name
)

select
    date,
    page,
    metric_name,
    sample_size,

    good_count,
    needs_improvement_count,
    poor_count,
    unique_sessions,
    unique_users,
    round(avg_value, 2) as avg_value,
    round(p50_value, 2) as p50_value,

    round(p75_value, 2) as p75_value,
    round(p95_value, 2) as p95_value,
    round(p99_value, 2) as p99_value,

    round(min_value, 2) as min_value,
    round(max_value, 2) as max_value,
    round(cast(good_count as float64) / nullif(sample_size, 0) * 100, 2)
        as good_pct,

    round(cast(needs_improvement_count as float64) / nullif(sample_size, 0) * 100, 2)
        as needs_improvement_pct,
    round(cast(poor_count as float64) / nullif(sample_size, 0) * 100, 2)
        as poor_pct,

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

    case
        when p95_value > (avg_value * 2) then 'high_variability'
        when p95_value > (avg_value * 1.5) then 'moderate_variability'
        else 'low_variability'
    end as variability_level

from daily_metrics
where sample_size >= 5

order by date desc, metric_name asc, page asc
