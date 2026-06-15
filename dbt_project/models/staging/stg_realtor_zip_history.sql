{{ config(enabled=false) }}

SELECT *
FROM {{ source('staging', 'realtor_zip_raw') }}
