{{
  config(
    materialized='table',
    description='ETF daily data aggregated to monthly averages with forward-looking quarterly percentage changes'
  )
}}


WITH monthly_averages AS (
  SELECT 
    symbol,
    exchange,
    EXTRACT(year FROM date) as year_val,
    EXTRACT(month FROM date) as month_val,
    CONCAT(EXTRACT(year FROM date), '-', LPAD(EXTRACT(month FROM date)::VARCHAR, 2, '0')) as year_month,
    MAKE_DATE(EXTRACT(year FROM date), EXTRACT(month FROM date), 1) as month_date,
    
    -- Monthly averages for key price metrics
    ROUND(AVG(close), 4) as avg_close,
    ROUND(AVG(open), 4) as avg_open, 
    ROUND(AVG(high), 4) as avg_high,
    ROUND(AVG(low), 4) as avg_low,
    ROUND(AVG(volume), 0) as avg_volume
    
  FROM {{ ref('stg_us_sectors') }}
  GROUP BY 
    symbol, 
    exchange,
    EXTRACT(year FROM date), 
    EXTRACT(month FROM date)
),

quarterly_data AS (
  SELECT 
    symbol,
    exchange,
    year_val,
    CASE 
      WHEN month_val IN (1, 2, 3) THEN 1
      WHEN month_val IN (4, 5, 6) THEN 2
      WHEN month_val IN (7, 8, 9) THEN 3
      WHEN month_val IN (10, 11, 12) THEN 4
    END as quarter_num,
    CONCAT(year_val, '-Q', 
      CASE 
        WHEN month_val IN (1, 2, 3) THEN 1
        WHEN month_val IN (4, 5, 6) THEN 2
        WHEN month_val IN (7, 8, 9) THEN 3
        WHEN month_val IN (10, 11, 12) THEN 4
      END
    ) as year_quarter,
    
    -- Average the monthly averages within each quarter
    AVG(avg_close) OVER (
      PARTITION BY symbol, exchange, year_val, 
      CASE 
        WHEN month_val IN (1, 2, 3) THEN 1
        WHEN month_val IN (4, 5, 6) THEN 2
        WHEN month_val IN (7, 8, 9) THEN 3
        WHEN month_val IN (10, 11, 12) THEN 4
      END
    ) as quarterly_avg_close,
    
    AVG(avg_open) OVER (
      PARTITION BY symbol, exchange, year_val, 
      CASE 
        WHEN month_val IN (1, 2, 3) THEN 1
        WHEN month_val IN (4, 5, 6) THEN 2
        WHEN month_val IN (7, 8, 9) THEN 3
        WHEN month_val IN (10, 11, 12) THEN 4
      END
    ) as quarterly_avg_open,
    
    AVG(avg_high) OVER (
      PARTITION BY symbol, exchange, year_val, 
      CASE 
        WHEN month_val IN (1, 2, 3) THEN 1
        WHEN month_val IN (4, 5, 6) THEN 2
        WHEN month_val IN (7, 8, 9) THEN 3
        WHEN month_val IN (10, 11, 12) THEN 4
      END
    ) as quarterly_avg_high,
    
    AVG(avg_low) OVER (
      PARTITION BY symbol, exchange, year_val, 
      CASE 
        WHEN month_val IN (1, 2, 3) THEN 1
        WHEN month_val IN (4, 5, 6) THEN 2
        WHEN month_val IN (7, 8, 9) THEN 3
        WHEN month_val IN (10, 11, 12) THEN 4
      END
    ) as quarterly_avg_low,
    
    AVG(avg_volume) OVER (
      PARTITION BY symbol, exchange, year_val, 
      CASE 
        WHEN month_val IN (1, 2, 3) THEN 1
        WHEN month_val IN (4, 5, 6) THEN 2
        WHEN month_val IN (7, 8, 9) THEN 3
        WHEN month_val IN (10, 11, 12) THEN 4
      END
    ) as quarterly_avg_volume,
    
    -- Keep monthly detail
    year_month,
    month_date,
    avg_close,
    avg_open,
    avg_high,
    avg_low,
    avg_volume
    
  FROM monthly_averages
),

with_forward_quarters AS (
  SELECT 
    *,
    -- Forward-looking quarterly close prices for percent change calculations
    LEAD(quarterly_avg_close, 1) OVER (
      PARTITION BY symbol, exchange 
      ORDER BY year_val, quarter_num
    ) as close_q1_forward,
    
    LEAD(quarterly_avg_close, 2) OVER (
      PARTITION BY symbol, exchange 
      ORDER BY year_val, quarter_num
    ) as close_q2_forward,
    
    LEAD(quarterly_avg_close, 3) OVER (
      PARTITION BY symbol, exchange 
      ORDER BY year_val, quarter_num
    ) as close_q3_forward,
    
    LEAD(quarterly_avg_close, 4) OVER (
      PARTITION BY symbol, exchange 
      ORDER BY year_val, quarter_num
    ) as close_q4_forward
    
  FROM quarterly_data
)

SELECT 
  symbol,
  exchange,
  year_month,
  month_date,
  year_quarter,
  quarter_num,
  year_val,
  
  -- Monthly averages
  avg_close as monthly_avg_close,
  avg_open as monthly_avg_open,
  avg_high as monthly_avg_high, 
  avg_low as monthly_avg_low,
  avg_volume as monthly_avg_volume,
  
  -- Quarterly averages
  ROUND(quarterly_avg_close, 4) as quarterly_avg_close,
  ROUND(quarterly_avg_open, 4) as quarterly_avg_open,
  ROUND(quarterly_avg_high, 4) as quarterly_avg_high,
  ROUND(quarterly_avg_low, 4) as quarterly_avg_low,
  ROUND(quarterly_avg_volume, 0) as quarterly_avg_volume,
  
  -- Forward quarterly percentage changes
  ROUND(
    (close_q1_forward - quarterly_avg_close) / quarterly_avg_close * 100, 2
  ) as pct_change_q1_forward,
  
  ROUND(
    (close_q2_forward - quarterly_avg_close) / quarterly_avg_close * 100, 2
  ) as pct_change_q2_forward,
  
  ROUND(
    (close_q3_forward - quarterly_avg_close) / quarterly_avg_close * 100, 2
  ) as pct_change_q3_forward,
  
  ROUND(
    (close_q4_forward - quarterly_avg_close) / quarterly_avg_close * 100, 2
  ) as pct_change_q4_forward

FROM with_forward_quarters
ORDER BY symbol, exchange, month_date