{{ config(
    materialized='view'
) }}

SELECT
    commodity_name,
    commodity_unit,
    CAST(commodity_price AS DECIMAL) AS price,
    CAST(date AS date) AS date
FROM {{ source('staging', 'input_commodities_raw') }}
WHERE commodity_price IS NOT NULL
  AND date IS NOT NULL

