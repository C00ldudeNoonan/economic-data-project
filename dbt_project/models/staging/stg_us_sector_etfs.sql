{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ source('staging', 'us_sector_etfs_raw') }}
