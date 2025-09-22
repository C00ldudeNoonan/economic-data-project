{{
  config(
    materialized='table',
    description='Analysis of economic data rate of change vs future stock returns, providing correlation analysis and quintile performance insights'
  )
}}

-- Analysis of Economic Data Rate of Change vs Future Stock Returns
WITH economic_changes AS (
  SELECT 
    symbol,
    month_date,
    series_name,
    category,
    value as current_econ_value,
    monthly_avg_close,
    pct_change_q1_forward,
    pct_change_q2_forward,
    pct_change_q3_forward,
    
    -- Calculate month-over-month change in economic data
    LAG(value, 1) OVER (PARTITION BY symbol, series_name ORDER BY month_date) as prev_econ_value,
    
    -- Calculate rate of change (month-over-month %)
    CASE 
      WHEN LAG(value, 1) OVER (PARTITION BY symbol, series_name ORDER BY month_date) IS NOT NULL 
           AND LAG(value, 1) OVER (PARTITION BY symbol, series_name ORDER BY month_date) != 0
      THEN ((value - LAG(value, 1) OVER (PARTITION BY symbol, series_name ORDER BY month_date)) / 
            LAG(value, 1) OVER (PARTITION BY symbol, series_name ORDER BY month_date)) * 100
      ELSE NULL
    END as econ_mom_change_pct,
    
    -- Calculate 3-month rolling average of economic change
    AVG(value) OVER (
      PARTITION BY symbol, series_name 
      ORDER BY month_date 
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as econ_3mo_avg
    
  FROM {{ ref('base_historical_analysis') }}
  WHERE value IS NOT NULL
),

correlation_analysis AS (
  SELECT 
    symbol,
    series_name,
    category,
    
    -- Basic correlation metrics between economic change and future returns
    COUNT(*) as observation_count,
    
    -- Correlation between economic MoM change and Q1 forward returns
    CORR(econ_mom_change_pct, pct_change_q1_forward) as corr_econ_q1_returns,
    CORR(econ_mom_change_pct, pct_change_q2_forward) as corr_econ_q2_returns,
    CORR(econ_mom_change_pct, pct_change_q3_forward) as corr_econ_q3_returns,
    
    -- Average returns when economic data is growing vs declining
    AVG(CASE WHEN econ_mom_change_pct > 0 THEN pct_change_q1_forward END) as avg_q1_return_when_econ_growing,
    AVG(CASE WHEN econ_mom_change_pct < 0 THEN pct_change_q1_forward END) as avg_q1_return_when_econ_declining,
    
    -- Standard deviations
    STDDEV(econ_mom_change_pct) as econ_change_volatility,
    STDDEV(pct_change_q1_forward) as q1_return_volatility,
    
    -- Economic data statistics
    AVG(econ_mom_change_pct) as avg_econ_change_pct,
    MIN(econ_mom_change_pct) as min_econ_change_pct,
    MAX(econ_mom_change_pct) as max_econ_change_pct
    
  FROM economic_changes
  WHERE econ_mom_change_pct IS NOT NULL
  GROUP BY symbol, series_name, category
),

detailed_monthly_view AS (
  SELECT 
    symbol,
    month_date,
    series_name,
    econ_mom_change_pct,
    pct_change_q1_forward,
    pct_change_q2_forward,
    pct_change_q3_forward,
    
    -- Quintile ranking of economic changes
    NTILE(5) OVER (PARTITION BY symbol, series_name ORDER BY econ_mom_change_pct) as econ_change_quintile,
    
    -- Lead/lag analysis - how does economic data relate to past and future returns
    LAG(pct_change_q1_forward, 1) OVER (PARTITION BY symbol ORDER BY month_date) as prev_month_q1_return,
    LEAD(pct_change_q1_forward, 1) OVER (PARTITION BY symbol ORDER BY month_date) as next_month_q1_return
    
  FROM economic_changes
  WHERE econ_mom_change_pct IS NOT NULL
)

-- Main Results: Show correlations and key insights
SELECT 
  'Correlation Analysis' as analysis_type,
  symbol,
  series_name,
  observation_count,
  ROUND(corr_econ_q1_returns, 4) as correlation_econ_vs_q1_returns,
  ROUND(corr_econ_q2_returns, 4) as correlation_econ_vs_q2_returns,
  ROUND(corr_econ_q3_returns, 4) as correlation_econ_vs_q3_returns,
  ROUND(avg_q1_return_when_econ_growing, 2) as avg_q1_return_econ_up,
  ROUND(avg_q1_return_when_econ_declining, 2) as avg_q1_return_econ_down,
  ROUND(avg_q1_return_when_econ_growing - avg_q1_return_when_econ_declining, 2) as return_difference
FROM correlation_analysis
WHERE observation_count >= 10  -- Filter for meaningful sample sizes

UNION ALL

-- Quintile Analysis: Performance by economic data change quintiles
SELECT 
  'Quintile Analysis' as analysis_type,
  symbol,
  series_name,
  NULL as observation_count,
  econ_change_quintile as correlation_econ_vs_q1_returns,
  NULL as correlation_econ_vs_q2_returns, 
  NULL as correlation_econ_vs_q3_returns,
  ROUND(AVG(pct_change_q1_forward), 2) as avg_q1_return_econ_up,
  COUNT(*) as avg_q1_return_econ_down,
  ROUND(AVG(econ_mom_change_pct), 2) as return_difference
FROM detailed_monthly_view
GROUP BY symbol, series_name, econ_change_quintile
HAVING COUNT(*) >= 3
