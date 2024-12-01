SELECT
  STRPTIME(LEFT(REPLACE(CAST(month_date_yyyymm AS VARCHAR), ',', ''), 4) || '-' || SUBSTRING(REPLACE(CAST(month_date_yyyymm AS VARCHAR), ',', ''), 5, 2) || '-01', '%Y-%m-%d') as year_month,
  state,   
  total_listing_count,
  total_listing_count_mm,
  total_listing_count_yy,
  average_listing_price,
  average_listing_price_mm,
  average_listing_price_yy,
FROM main.RDC_Inventory_Core_Metrics_State
WHERE STRPTIME(LEFT(REPLACE(CAST(month_date_yyyymm AS VARCHAR), ',', ''), 4) || '-' || SUBSTRING(REPLACE(CAST(month_date_yyyymm AS VARCHAR), ',', ''), 5, 2) || '-01', '%Y-%m-%d') = (SELECT MAX(STRPTIME(LEFT(REPLACE(CAST(month_date_yyyymm AS VARCHAR), ',', ''), 4) || '-' || SUBSTRING(REPLACE(CAST(month_date_yyyymm AS VARCHAR), ',', ''), 5, 2) || '-01', '%Y-%m-%d')) FROM main.RDC_Inventory_Core_Metrics_State);