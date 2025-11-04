{{ config(
    materialized='view'
) }}

SELECT
    commodity_name,
    commodity_unit,
    cast(commodity_price AS decimal) AS price,
    cast(date AS date) AS date
FROM {{ source('staging', 'agriculture_commodities_raw') }}
WHERE
    commodity_price IS NOT NULL
    AND date IS NOT NULL
