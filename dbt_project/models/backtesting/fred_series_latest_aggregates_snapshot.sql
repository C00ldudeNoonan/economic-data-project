{{ config(
    materialized='incremental',
    unique_key=['snapshot_date', 'series_code', 'month'],
    incremental_strategy='delete+insert'
) }}

WITH snapshot_dates AS (
    -- Generate snapshot dates (first day of each month) from available data
    SELECT DISTINCT
        DATE_TRUNC('month', date) AS snapshot_date
    FROM {{ ref('stg_fred_series') }}
    WHERE date >= '2020-01-01'  -- Adjust based on your data availability
),

date_bounds AS (
    SELECT
        snapshot_date AS end_date,
        snapshot_date - INTERVAL '12 months' AS start_date,
        snapshot_date
    FROM snapshot_dates
),

series_dates AS (
    SELECT
        db.snapshot_date,
        fred_data.series_code,
        fred_data.series_name,
        LAG(CAST(NULLIF(fred_data.value, '.') AS FLOAT), -2)
            OVER (PARTITION BY db.snapshot_date, fred_data.series_code ORDER BY fred_data.date DESC)
            AS previous_date,
        LAG(CAST(NULLIF(fred_data.value, '.') AS FLOAT), -3)
            OVER (PARTITION BY db.snapshot_date, fred_data.series_code ORDER BY fred_data.date DESC)
            AS two_events_ago
    FROM {{ ref('stg_fred_series') }} AS fred_data
    CROSS JOIN date_bounds AS db
    WHERE fred_data.date >= db.start_date AND fred_data.date <= db.end_date
),

date_grain AS (
    SELECT
        snapshot_date,
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
        END AS date_grain
    FROM series_dates AS s
    GROUP BY snapshot_date, s.series_code, s.series_name
),

aggregates AS (
    SELECT
        db.snapshot_date,
        fred_data.series_code,
        fred_data.series_name,
        date_grain.date_grain,
        DATE_TRUNC('month', fred_data.date) AS month,
        ROUND(AVG(CAST(NULLIF(fred_data.value, '.') AS FLOAT)), 4) AS clean_value
    FROM {{ ref('stg_fred_series') }} AS fred_data
    CROSS JOIN date_bounds AS db
    LEFT JOIN date_grain
        ON db.snapshot_date = date_grain.snapshot_date
        AND fred_data.series_code = date_grain.series_code
    WHERE fred_data.date >= db.start_date 
        AND fred_data.date <= db.end_date
        AND date_grain.date_grain IN ('Daily', 'Monthly', 'Quarterly', 'Weekly')
    GROUP BY
        db.snapshot_date,
        DATE_TRUNC('month', fred_data.date),
        fred_data.series_code,
        date_grain.date_grain,
        fred_data.series_name
),

date_ranges AS (
    SELECT
        snapshot_date,
        series_code,
        series_name,
        date_grain,
        month,
        clean_value,
        LAG(clean_value, 3) OVER (
            PARTITION BY snapshot_date, series_code
            ORDER BY month
        ) AS value_3m_ago,
        LAG(clean_value, 6) OVER (
            PARTITION BY snapshot_date, series_code
            ORDER BY month
        ) AS value_6m_ago,
        LAG(clean_value, 12) OVER (
            PARTITION BY snapshot_date, series_code
            ORDER BY month
        ) AS value_1y_ago
    FROM aggregates
),

calc_view AS (
    SELECT
        snapshot_date,
        series_code,
        series_name,
        date_grain,
        month,
        clean_value AS current_value,
        value_3m_ago,
        value_6m_ago,
        value_1y_ago,
        CASE
            WHEN value_3m_ago IS NULL OR value_3m_ago = 0 THEN NULL
            ELSE ROUND((clean_value - value_3m_ago) / (value_3m_ago), 2)
        END AS pct_change_3m,
        CASE
            WHEN value_6m_ago IS NULL OR value_6m_ago = 0 THEN NULL
            ELSE ROUND((clean_value - value_6m_ago) / (value_6m_ago), 2)
        END AS pct_change_6m,
        CASE
            WHEN value_1y_ago IS NULL OR value_1y_ago = 0 THEN NULL
            ELSE ROUND((clean_value - value_1y_ago) / (value_1y_ago), 2)
        END AS pct_change_1y
    FROM date_ranges
),

max_date_view AS (
    SELECT
        snapshot_date,
        series_code,
        MAX(month) AS month
    FROM calc_view
    GROUP BY snapshot_date, series_code
),

final AS (
    SELECT
        calc_view.snapshot_date,
        calc_view.series_code,
        calc_view.series_name,
        calc_view.month,
        calc_view.current_value,
        calc_view.pct_change_3m,
        calc_view.pct_change_6m,
        calc_view.pct_change_1y,
        calc_view.date_grain
    FROM calc_view
    INNER JOIN max_date_view
        ON calc_view.snapshot_date = max_date_view.snapshot_date
        AND calc_view.series_code = max_date_view.series_code
        AND calc_view.month = max_date_view.month
)

SELECT * FROM final
ORDER BY snapshot_date DESC, series_code

