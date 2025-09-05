{{ config(
    materialized='table'
) }}


WITH date_bounds AS (
    SELECT
        CURRENT_DATE AS end_date,
        CURRENT_DATE - INTERVAL '12 months' AS start_date
),

series_dates AS (
    SELECT
        series_code,
        series_name,
        date,
        LAG(date, -2)
            OVER (
                PARTITION BY series_code
                ORDER BY date DESC
            )
            AS previous_date,
        LAG(date, -3)
            OVER (
                PARTITION BY series_code
                ORDER BY date DESC
            )
            AS two_events_ago
    FROM {{ ref('stg_fred_series') }} AS s, date_bounds AS d
    WHERE s.date >= d.start_date AND s.date <= d.end_date
)

SELECT
    s.series_code,
    s.series_name,
    COUNT(*) AS entry_count,
    CASE
        WHEN COUNT(*) >= 200 THEN 'Daily'
        WHEN COUNT(*) >= 50 THEN 'Weekly'
        WHEN COUNT(*) >= 9 THEN 'Monthly'
        WHEN COUNT(*) >= 2 THEN 'Quarterly'
        WHEN COUNT(*) >= 1 THEN 'Annually'
        ELSE 'Limited Data'
    END AS coverage_status
FROM
    series_dates AS s
GROUP BY
    s.series_code,
    s.series_name
ORDER BY
    entry_count DESC
