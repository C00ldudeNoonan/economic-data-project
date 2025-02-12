{{ config(
    materialized='view'
) }}

SELECT
    fr.date,
    fr.value,
    fr.series_code,
    fr.literal,
    map.series_name
FROM {{ source('staging', 'fred_data_raw') }} fr
LEFT JOIN {{ ref('fred_series_mapping') }} map
    ON fr.series_code = map.code