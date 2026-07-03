{{ config(materialized='table') }}

SELECT date_day
FROM UNNEST(
    GENERATE_DATE_ARRAY(
        DATE '2000-01-01',
        DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY),
        INTERVAL 1 DAY
    )
) AS date_day
