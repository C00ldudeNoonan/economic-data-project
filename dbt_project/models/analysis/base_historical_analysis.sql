{{
  config(
    materialized='table',
    description='Comprehensive historical analysis combining market return data across all asset categories with economic indicators from FRED'
  )
}}

WITH return_data as (
  SELECT
    symbol,
    month_date,
    quarter_num,
    year_val,
    monthly_avg_close,
    monthly_avg_volume,
    quarterly_avg_close,
    quarterly_avg_volume,
    pct_change_q1_forward,
    pct_change_q2_forward,
    pct_change_q3_forward,
    pct_change_q4_forward,
    'currency' as category
  FROM {{ ref('currency_analysis_return') }}
  
  UNION ALL

  SELECT
    symbol,
    month_date,
    quarter_num,
    year_val,
    monthly_avg_close,
    monthly_avg_volume,
    quarterly_avg_close,
    quarterly_avg_volume,
    pct_change_q1_forward,
    pct_change_q2_forward,
    pct_change_q3_forward,
    pct_change_q4_forward,
    'fixed_income' as category
  FROM {{ ref('fixed_income_analysis_return') }}

  UNION ALL
  
  SELECT
    symbol,
    month_date,
    quarter_num,
    year_val,
    monthly_avg_close,
    monthly_avg_volume,
    quarterly_avg_close,
    quarterly_avg_volume,
    pct_change_q1_forward,
    pct_change_q2_forward,
    pct_change_q3_forward,
    pct_change_q4_forward,
    'global_markets' as category
  FROM {{ ref('global_markets_analysis_return') }}

  UNION ALL
  
  SELECT
    symbol,
    month_date,
    quarter_num,
    year_val,
    monthly_avg_close,
    monthly_avg_volume,
    quarterly_avg_close,
    quarterly_avg_volume,
    pct_change_q1_forward,
    pct_change_q2_forward,
    pct_change_q3_forward,
    pct_change_q4_forward,
    'major_indicies' as category
  FROM {{ ref('major_indicies_analysis_return') }}

  UNION ALL
  
  SELECT
    symbol,
    month_date,
    quarter_num,
    year_val,
    monthly_avg_close,
    monthly_avg_volume,
    quarterly_avg_close,
    quarterly_avg_volume,
    pct_change_q1_forward,
    pct_change_q2_forward,
    pct_change_q3_forward,
    pct_change_q4_forward,
    'sector' as category
  FROM {{ ref('us_sector_analysis_return') }}

),

SELECT 
  rt.symbol,
  rt.month_date,
  rt.quarter_num,
  rt.year_val,
  rt.monthly_avg_close,
  rt.monthly_avg_volume,
  rt.quarterly_avg_close,
  rt.quarterly_avg_volume,
  rt.pct_change_q1_forward,
  rt.pct_change_q2_forward,
  rt.pct_change_q3_forward,
  rt.pct_change_q4_forward,
  rt.category,
  fr.series_name,
  fr.value,
  fr.period_diff
FROM return_data rt
LEFT JOIN {{ ref('fred_monthly_diff') }} fr
  on rt.month_date = cast(fr.date as date) 
