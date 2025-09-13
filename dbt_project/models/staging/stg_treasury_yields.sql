{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ source('staging', 'treasury_yields_raw') }}
