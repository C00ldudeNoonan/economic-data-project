{{ config(enabled=false) }}

SELECT *
FROM {{ source('staging', 'realtor_metro_raw') }}
