{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ source('staging', 'major_indices_raw') }}
