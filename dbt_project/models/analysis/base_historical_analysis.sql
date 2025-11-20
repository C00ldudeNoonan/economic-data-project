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

quarterly_returns AS (
  SELECT
    *,
    -- Get first month's close price in the quarter (earliest date)
    FIRST_VALUE(monthly_avg_close) OVER (
      PARTITION BY symbol, category, year_val, quarter_num
      ORDER BY month_date ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS quarter_first_month_close,
    -- Get last month's close price in the quarter (latest date)
    LAST_VALUE(monthly_avg_close) OVER (
      PARTITION BY symbol, category, year_val, quarter_num
      ORDER BY month_date ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS quarter_last_month_close
  FROM return_data
)

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
  -- Calculate quarterly total return: (last month - first month) / first month * 100
  CASE
    WHEN rt.quarter_first_month_close IS NOT NULL 
         AND rt.quarter_first_month_close > 0
         AND rt.quarter_last_month_close IS NOT NULL
    THEN ROUND(
      (rt.quarter_last_month_close - rt.quarter_first_month_close) / rt.quarter_first_month_close * 100, 
      2
    )
    ELSE NULL
  END AS quarterly_total_return_pct,
  rt.quarter_first_month_close,
  rt.quarter_last_month_close,
  fr.series_name,
  fr.value,
  fr.period_diff
FROM quarterly_returns rt
LEFT JOIN {{ ref('fred_monthly_diff') }} fr
  on rt.month_date = cast(fr.date as date) 
