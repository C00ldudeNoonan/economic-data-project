{{ config(
    materialized='table'
) }}


WITH date_bounds AS (
    SELECT 
        CURRENT_DATE as end_date,
        CURRENT_DATE - INTERVAL '12 months' as start_date
),
series_dates AS (
    SELECT 
        series_code,
        series_name,
        date,
        LAG(date, -2) OVER (PARTITION BY series_code ORDER BY date DESC) as previous_date,
        LAG(date, -3) OVER (PARTITION BY series_code ORDER BY date DESC) as two_events_ago
    FROM {{ref('stg_fred_series')}} s, date_bounds d
    WHERE s.date >= d.start_date AND s.date <= d.end_date
)
SELECT 
    s.series_code,
    s.series_name,
    COUNT(*) as entry_count,
    CASE 
        WHEN COUNT(*) >= 200 THEN 'Daily'
        WHEN COUNT(*) >= 50 THEN 'Weekly'
        WHEN COUNT(*) >= 9 THEN 'Monthly'
        WHEN COUNT(*) >= 2 THEN 'Quarterly'
        WHEN COUNT(*) >= 1 THEN 'Annually'
        ELSE 'Limited Data'
    END as coverage_status
FROM 
    series_dates s
GROUP BY s.series_code,
    s.series_name
ORDER BY 
    entry_count DESC