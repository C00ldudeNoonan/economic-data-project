{{
    config(
        tags=['staging', 'events', 'earnings']
    )
}}

with source as (
    select * from {{ source('staging', 'earnings_calendar') }}
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
        try_cast(report_date as date) as report_date,
        -- Computed fields
        extract(year from try_cast(report_date as date)) as year,
        extract(month from try_cast(report_date as date)) as month,
        extract(week from try_cast(report_date as date)) as week_of_year,
        extract(dow from try_cast(report_date as date)) as day_of_week,
        -- EPS surprise calculation
        case
            when try_cast(eps_actual as double) is not null and try_cast(eps_estimated as double) is not null and try_cast(eps_estimated as double) != 0
                then ((try_cast(eps_actual as double) - try_cast(eps_estimated as double)) / abs(try_cast(eps_estimated as double))) * 100
        end as eps_surprise_pct,
        -- Beat/miss indicator
        case
            when try_cast(eps_actual as double) is not null and try_cast(eps_estimated as double) is not null
                then
                    case
                        when try_cast(eps_actual as double) > try_cast(eps_estimated as double) then 'beat'
                        when try_cast(eps_actual as double) < try_cast(eps_estimated as double) then 'miss'
                        else 'met'
                    end
        end as eps_result,
        -- Revenue surprise
        case
            when try_cast(revenue_actual as double) is not null and try_cast(revenue_estimated as double) is not null and try_cast(revenue_estimated as double) != 0
                then ((try_cast(revenue_actual as double) - try_cast(revenue_estimated as double)) / abs(try_cast(revenue_estimated as double))) * 100
        end as revenue_surprise_pct,
        -- Upcoming flag
        coalesce(try_cast(report_date as date) > current_date, false) as is_upcoming,
        -- Days until report
        date_diff('day', current_date, try_cast(report_date as date)) as days_until_report,
        -- Has reported flag
        coalesce(eps_actual is not null, false) as has_reported
    from source
    where report_date is not null
)

select * from cleaned
