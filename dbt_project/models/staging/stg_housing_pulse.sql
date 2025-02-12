{{ config(
    materialized='view'
) }}

SELECT
    SURVEY_YEAR,
    NAME,
    MEASURE_NAME,
    COL_START_DATE,
    COL_END_DATE,
    RATE,
    TOTAL,
    MEASURE_DESCRIPTION,
    time,
    CYCLE,
    state
FROM {{ source('staging', 'housing_pulse_raw') }}