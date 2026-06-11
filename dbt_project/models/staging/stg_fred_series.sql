SELECT
    CAST(fr.date AS DATE) AS date,
    SAFE_CAST(NULLIF(fr.value, '.') AS FLOAT64) AS value,
    fr.series_code,
    fr.literal,
    map.series_name,
    map.category
FROM {{ source('staging', 'fred_raw') }} AS fr
LEFT JOIN {{ ref('fred_series_mapping') }} AS map
    ON fr.series_code = map.code
