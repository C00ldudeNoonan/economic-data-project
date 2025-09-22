{{ config(
    materialized='view'
) }}

SELECT
    fr.date,
    fr.value,
    fr.series_code,
    fr.literal,
    map.series_name,
    map.category
FROM {{ source('staging', 'fred_raw') }} AS fr
LEFT JOIN {{ ref('fred_series_mapping') }} AS map
    ON fr.series_code = map.code
