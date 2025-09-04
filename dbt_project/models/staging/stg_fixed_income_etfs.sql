{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ source('staging', 'fixed_income_etfs_raw') }}
