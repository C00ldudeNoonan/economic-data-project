{{
  config(
    materialized='table',
    description='Economic time series data with monthly grain and simple difference imputation'
  )
}}

WITH monthly_data AS (
    SELECT
        series_code,
        series_name,
        date,
        literal AS value,
        EXTRACT(YEAR FROM date) AS year_val,
        EXTRACT(MONTH FROM date) AS month_val
    FROM {{ ref('stg_fred_series') }}
),

date_bounds AS (
    SELECT
        series_code,
        series_name,
        MIN(date) AS min_date,
        MAX(date) AS max_date
    FROM monthly_data
    GROUP BY series_code, series_name
),

all_dates AS (
    SELECT
        db.series_code,
        db.series_name,
        dates.date
    FROM date_bounds AS db
    CROSS JOIN (
        SELECT UNNEST(
            GENERATE_SERIES(
                (SELECT MIN(min_date) FROM date_bounds),
                (SELECT MAX(max_date) FROM date_bounds),
                INTERVAL '1 month'
            )
        ) AS date
    ) AS dates
    WHERE dates.date >= db.min_date AND dates.date <= db.max_date
),

data_with_gaps AS (
    SELECT
        ad.series_code,
        ad.series_name,
        ad.date,
        md.value AS actual_value
    FROM all_dates AS ad
    LEFT JOIN monthly_data AS md
        ON
            ad.series_code = md.series_code
            AND ad.date = md.date
),

with_neighbors AS (
    SELECT
        *,
        LAG(actual_value) OVER (
            PARTITION BY series_code ORDER BY date
        ) AS prev_actual,
        LEAD(actual_value) OVER (
            PARTITION BY series_code ORDER BY date
        ) AS next_actual
    FROM data_with_gaps
),

interpolated AS (
    SELECT
        series_code,
        series_name,
        date,
        actual_value,
        CASE
            WHEN actual_value IS NOT NULL THEN actual_value
            WHEN prev_actual IS NOT NULL AND next_actual IS NOT NULL
                THEN (prev_actual + next_actual) / 2.0
            WHEN prev_actual IS NOT NULL
                THEN prev_actual  -- Forward fill
            WHEN next_actual IS NOT NULL
                THEN next_actual  -- Backward fill
        END AS imputed_value,
        CASE
            WHEN actual_value IS NOT NULL THEN 'Actual'
            WHEN prev_actual IS NOT NULL AND next_actual IS NOT NULL
                THEN 'Interpolated'
            WHEN prev_actual IS NOT NULL
                THEN 'Forward Filled'
            WHEN next_actual IS NOT NULL
                THEN 'Backward Filled'
            ELSE 'No Data'
        END AS data_source
    FROM with_neighbors
)

SELECT
    date,
    series_code,
    series_name,
    data_source,
    ROUND(imputed_value, 2) AS value,
    ROUND(
        imputed_value - LAG(imputed_value) OVER (
            PARTITION BY series_code
            ORDER BY date
        ),
        2
    ) AS period_diff
FROM interpolated
WHERE imputed_value IS NOT NULL
ORDER BY series_code, date
