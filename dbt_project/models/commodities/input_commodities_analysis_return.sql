{{
  config(
    materialized='table',
    description='Input/industrial commodities daily data aggregated to monthly averages with forward-looking quarterly percentage changes'
  )
}}

WITH monthly_averages AS (
    SELECT
        commodity_name,
        commodity_unit,
        EXTRACT(YEAR FROM date) AS year_val,
        EXTRACT(MONTH FROM date) AS month_val,
        CONCAT(
            EXTRACT(YEAR FROM date),
            '-',
            LPAD(EXTRACT(MONTH FROM date)::VARCHAR, 2, '0')
        ) AS year_month,
        MAKE_DATE(EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date), 1)
            AS month_date,
        ROUND(AVG(price), 4) AS avg_price
    FROM {{ ref('stg_input_commodities') }}
    GROUP BY
        commodity_name,
        commodity_unit,
        EXTRACT(YEAR FROM date),
        EXTRACT(MONTH FROM date)
),

quarterly_data AS (
    SELECT
        commodity_name,
        commodity_unit,
        year_val,
        year_month,
        month_date,
        avg_price,
        CASE
            WHEN month_val IN (1, 2, 3) THEN 1
            WHEN month_val IN (4, 5, 6) THEN 2
            WHEN month_val IN (7, 8, 9) THEN 3
            WHEN month_val IN (10, 11, 12) THEN 4
        END AS quarter_num,
        CONCAT(
            year_val, '-Q',
            CASE
                WHEN month_val IN (1, 2, 3) THEN 1
                WHEN month_val IN (4, 5, 6) THEN 2
                WHEN month_val IN (7, 8, 9) THEN 3
                WHEN month_val IN (10, 11, 12) THEN 4
            END
        ) AS year_quarter,
        AVG(avg_price) OVER (
            PARTITION BY
                commodity_name, commodity_unit, year_val,
                CASE
                    WHEN month_val IN (1, 2, 3) THEN 1
                    WHEN month_val IN (4, 5, 6) THEN 2
                    WHEN month_val IN (7, 8, 9) THEN 3
                    WHEN month_val IN (10, 11, 12) THEN 4
                END
        ) AS quarterly_avg_price
    FROM monthly_averages
),

with_forward_quarters AS (
    SELECT
        *,
        LEAD(quarterly_avg_price, 1) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY year_val, quarter_num
        ) AS price_q1_forward,
        LEAD(quarterly_avg_price, 2) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY year_val, quarter_num
        ) AS price_q2_forward,
        LEAD(quarterly_avg_price, 3) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY year_val, quarter_num
        ) AS price_q3_forward,
        LEAD(quarterly_avg_price, 4) OVER (
            PARTITION BY commodity_name, commodity_unit
            ORDER BY year_val, quarter_num
        ) AS price_q4_forward
    FROM quarterly_data
)

SELECT
    commodity_name,
    commodity_unit,
    year_month,
    month_date,
    year_quarter,
    quarter_num,
    year_val,
    avg_price AS monthly_avg_price,
    ROUND(quarterly_avg_price, 4) AS quarterly_avg_price,
    ROUND(
        (price_q1_forward - quarterly_avg_price) / quarterly_avg_price * 100, 2
    ) AS pct_change_q1_forward,
    ROUND(
        (price_q2_forward - quarterly_avg_price) / quarterly_avg_price * 100, 2
    ) AS pct_change_q2_forward,
    ROUND(
        (price_q3_forward - quarterly_avg_price) / quarterly_avg_price * 100, 2
    ) AS pct_change_q3_forward,
    ROUND(
        (price_q4_forward - quarterly_avg_price) / quarterly_avg_price * 100, 2
    ) AS pct_change_q4_forward
FROM with_forward_quarters
ORDER BY commodity_name, commodity_unit, month_date

