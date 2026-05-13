SELECT *
FROM {{ source('staging', 'realtor_country_raw') }}
