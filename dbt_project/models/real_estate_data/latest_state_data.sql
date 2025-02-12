{{ config(
    materialized='table'
) }}


SELECT
  year_month,
  state,   
  total_listing_count,
  total_listing_count_mm,
  total_listing_count_yy,
  average_listing_price,
  average_listing_price_mm,
  average_listing_price_yy,
FROM {{ref('stg_realtor_state_history')}}
WHERE year_month = (SELECT MAX(year_month) FROM {{ref('stg_realtor_state_history')}})