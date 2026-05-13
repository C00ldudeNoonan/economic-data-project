SELECT *
FROM {{ source('staging', 'realtor_county_raw') }}
