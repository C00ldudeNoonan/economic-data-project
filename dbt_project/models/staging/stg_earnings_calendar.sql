{{
    config(
        tags=['staging', 'events', 'earnings']
    )
}}

with source as (
    select * from {{ source('staging', 'earnings_calendar') }}
),

typed as (
    select
        *,
        safe_cast(report_date as date) as report_dt,
        safe_cast(eps_actual as float64) as eps_actual_float,
        safe_cast(eps_estimated as float64) as eps_estimated_float,
        safe_cast(revenue_actual as float64) as revenue_actual_float,
        safe_cast(revenue_estimated as float64) as revenue_estimated_float
    from source
    where report_date is not null
),

cleaned as (
    select
        event_id,
        symbol,
        company_name,
        fiscal_date_ending,
        eps_estimated,
        eps_actual,
        revenue_estimated,
        revenue_actual,
        report_time,
        timing,
        event_type,
        source,
        fetched_at,
        report_dt as report_date,
        -- Computed fields
        extract(year from report_dt) as year,
        extract(month from report_dt) as month,
        extract(week from report_dt) as week_of_year,
        extract(dayofweek from report_dt) as day_of_week,
        -- EPS surprise calculation
        case
            when eps_actual_float is not null and eps_estimated_float is not null and eps_estimated_float != 0
                then ((eps_actual_float - eps_estimated_float) / abs(eps_estimated_float)) * 100
        end as eps_surprise_pct,
        -- Beat/miss indicator
        case
            when eps_actual_float is not null and eps_estimated_float is not null
                then
                    case
                        when eps_actual_float > eps_estimated_float then 'beat'
                        when eps_actual_float < eps_estimated_float then 'miss'
                        else 'met'
                    end
        end as eps_result,
        -- Revenue surprise
        case
            when revenue_actual_float is not null and revenue_estimated_float is not null and revenue_estimated_float != 0
                then ((revenue_actual_float - revenue_estimated_float) / abs(revenue_estimated_float)) * 100
        end as revenue_surprise_pct,
        -- Upcoming flag
        coalesce(report_dt > current_date(), false) as is_upcoming,
        -- Days until report
        date_diff(report_dt, current_date(), day) as days_until_report,
        -- Has reported flag
        coalesce(eps_actual is not null, false) as has_reported
    from typed
)

select * from cleaned
