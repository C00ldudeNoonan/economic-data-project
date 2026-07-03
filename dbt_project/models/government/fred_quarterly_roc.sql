{{
  config(
    description='Economic time series data with monthly interpolation and percent change calculations'
  )
}}

WITH fred_monthly AS (
    SELECT
        series_code,
        series_name,
        EXTRACT(YEAR FROM date) AS year_val,
        EXTRACT(MONTH FROM date) AS month_val,
        DATE(EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date), 1)
            AS month_date,
        literal
    FROM {{ ref('stg_fred_series') }}
),

quarterly_data AS (
    SELECT
        series_code,
        series_name,
        CONCAT(year_val, '-', month_val) AS year_month,
        year_val,
        month_val,
        month_date,
        AVG(literal) AS avg_value
    FROM fred_monthly
    GROUP BY
        year_val,
        month_val,
        month_date,
        series_code,
        series_name
),

date_bounds AS (
    SELECT
        series_code,
        series_name,
        MIN(month_date) AS min_date,
        MAX(month_date) AS max_date
    FROM quarterly_data
    GROUP BY series_code, series_name
),

all_months AS (
    SELECT
        db.series_code,
        db.series_name,
        months AS month_date,
        CONCAT(
            EXTRACT(YEAR FROM months),
            '-',
            EXTRACT(MONTH FROM months)
        ) AS year_month
    FROM date_bounds AS db
    CROSS JOIN UNNEST(
        GENERATE_DATE_ARRAY(
            (SELECT MIN(min_date) FROM date_bounds),
            (SELECT MAX(max_date) FROM date_bounds),
            INTERVAL 1 MONTH
        )
    ) AS months
    WHERE months >= db.min_date AND months <= db.max_date
),

data_with_gaps AS (
    SELECT
        am.series_code,
        am.series_name,
        am.year_month,
        am.month_date,
        qd.avg_value AS actual_value
    FROM all_months AS am
    LEFT JOIN quarterly_data AS qd
        ON
            am.series_code = qd.series_code
            AND am.year_month = qd.year_month
),

with_neighbors AS (
    SELECT
        *,
        LAG(actual_value) OVER (
            PARTITION BY series_code ORDER BY month_date
        ) AS prev_actual,
        LEAD(actual_value) OVER (
            PARTITION BY series_code ORDER BY month_date
        ) AS next_actual
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
        END AS avg_value,
        CASE
            WHEN actual_value IS NOT NULL THEN 'Actual'
            WHEN
                prev_actual IS NOT NULL AND next_actual IS NOT NULL
                THEN 'Interpolated'
            ELSE 'No Data'
        END AS data_source
    FROM with_neighbors
)

SELECT
    year_month,
    series_code,
    series_name,
    data_source,
    month_date,
    ROUND(avg_value, 2) AS avg_value,
    ROUND(
        SAFE_DIVIDE(
            avg_value - LAG(avg_value) OVER (
                PARTITION BY series_code
                ORDER BY month_date
            ),
            LAG(avg_value) OVER (
                PARTITION BY series_code
                ORDER BY month_date
            )
        ) * 100,
        2
    ) AS pct_change_period
FROM interpolated
WHERE avg_value IS NOT NULL
ORDER BY series_code, month_date
