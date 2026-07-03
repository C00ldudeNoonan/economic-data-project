{{ config(enabled=false) }}

SELECT *
FROM {{ source('staging', 'realtor_country_raw') }}
