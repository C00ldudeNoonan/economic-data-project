SELECT *
FROM {{ source('staging', 'realtor_zip_raw') }}
