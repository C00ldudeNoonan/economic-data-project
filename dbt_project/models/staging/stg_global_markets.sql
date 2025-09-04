{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ source('staging', 'global_markets_raw') }}
