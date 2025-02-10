{{ config(
    materialized='table'
) }}


SELECT
CAST(
    SUBSTRING(CAST(month_date_yyyymm as VARCHAR), 1, 4) || '-' ||
    SUBSTRING(CAST(month_date_yyyymm as VARCHAR), 5, 2) || '-01'
    AS Date
    ) AS month_date,
county_fips,
county_name,
median_listing_price,
median_listing_price_mm,
median_listing_price_yy,
active_listing_count,
active_listing_count_mm,
active_listing_count_yy,
median_days_on_market,
median_days_on_market_mm,
median_days_on_market_yy,
new_listing_count,
new_listing_count_mm,
new_listing_count_yy,
price_increased_count,
price_increased_count_mm,
price_increased_count_yy,
price_reduced_count,
price_reduced_count_mm,
price_reduced_count_yy,
pending_listing_count,
pending_listing_count_mm,
pending_listing_count_yy,
median_listing_price_per_square_foot,
median_listing_price_per_square_foot_mm,
median_listing_price_per_square_foot_yy,
median_square_feet,
median_square_feet_mm,
median_square_feet_yy,
average_listing_price,
average_listing_price_mm,
average_listing_price_yy,
total_listing_count,
total_listing_count_mm,
total_listing_count_yy,
pending_ratio,
pending_ratio_mm,
pending_ratio_yy,
quality_flag
FROM {{ref('stg_realtor_country_history')}};
