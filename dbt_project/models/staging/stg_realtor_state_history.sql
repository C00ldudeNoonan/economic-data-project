{{ config(enabled=false) }}

SELECT *
FROM {{ source('staging', 'realtor_state_raw') }}
