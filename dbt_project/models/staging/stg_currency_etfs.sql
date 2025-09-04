{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ source('staging', 'currency_etfs_raw') }}
