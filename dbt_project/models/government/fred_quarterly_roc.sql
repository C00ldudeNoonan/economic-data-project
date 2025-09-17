{{
  config(
    materialized='table',
    description='Economic time series data with monthly interpolation and percent change calculations'
  )
}}

WITH quarterly_data AS (
  SELECT 
    CONCAT(EXTRACT(year FROM date), '-', EXTRACT(month FROM date)) as year_month,
    EXTRACT(year FROM date) as year_val,
    EXTRACT(month FROM date) as month_val,
    MAKE_DATE(EXTRACT(year FROM date), EXTRACT(month FROM date), 1) as month_date,
    series_code,
    series_name,
    avg(literal) as avg_value
  FROM {{ ref('stg_fred_series') }}
  GROUP BY EXTRACT(year FROM date), EXTRACT(month FROM date), series_code, series_name
),

date_bounds AS (
  SELECT 
    series_code,
    series_name,
    MIN(month_date) as min_date,
    MAX(month_date) as max_date
  FROM quarterly_data
  GROUP BY series_code, series_name
),

all_months AS (
  SELECT 
    db.series_code,
    db.series_name,
    month_date,
    CONCAT(EXTRACT(year FROM month_date), '-', EXTRACT(month FROM month_date)) as year_month
  FROM date_bounds db
  CROSS JOIN (
    SELECT UNNEST(
      generate_series(
        (SELECT MIN(min_date) FROM date_bounds),
        (SELECT MAX(max_date) FROM date_bounds),
        INTERVAL '1 month'
      )
    ) as month_date
  ) months
  WHERE month_date >= db.min_date AND month_date <= db.max_date
),

data_with_gaps AS (
  SELECT 
    am.series_code,
    am.series_name,
    am.year_month,
    am.month_date,
    qd.avg_value as actual_value
  FROM all_months am
  LEFT JOIN quarterly_data qd 
    ON am.series_code = qd.series_code 
    AND am.year_month = qd.year_month
),

with_neighbors AS (
  SELECT 
    *,
    LAG(CASE WHEN actual_value IS NOT NULL THEN actual_value END) OVER (
      PARTITION BY series_code ORDER BY month_date
    ) as prev_actual,
    LEAD(CASE WHEN actual_value IS NOT NULL THEN actual_value END) OVER (
      PARTITION BY series_code ORDER BY month_date
    ) as next_actual
  FROM data_with_gaps
),

interpolated AS (
  SELECT 
    series_code,
    series_name,
    year_month,
    month_date,
    actual_value,
    CASE 
      WHEN actual_value IS NOT NULL THEN actual_value
      WHEN prev_actual IS NOT NULL AND next_actual IS NOT NULL 
      THEN (prev_actual + next_actual) / 2.0
      ELSE NULL
    END as avg_value,
    CASE 
      WHEN actual_value IS NOT NULL THEN 'Actual'
      WHEN prev_actual IS NOT NULL AND next_actual IS NOT NULL THEN 'Interpolated'
      ELSE 'No Data'
    END as data_source
  FROM with_neighbors
)

SELECT 
  year_month,
  series_code,
  series_name,
  ROUND(avg_value, 2) as avg_value,
  ROUND(
    (avg_value - LAG(avg_value) OVER (
      PARTITION BY series_code 
      ORDER BY month_date
    )) / LAG(avg_value) OVER (
      PARTITION BY series_code 
      ORDER BY month_date
    ) * 100, 
    2
  ) as pct_change_period,
  data_source,
  month_date
FROM interpolated
WHERE avg_value IS NOT NULL
ORDER BY series_code, month_date