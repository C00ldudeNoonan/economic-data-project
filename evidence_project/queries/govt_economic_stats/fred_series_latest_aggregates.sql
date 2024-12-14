WITH date_bounds AS (
    SELECT 
        CURRENT_DATE as end_date,
        CURRENT_DATE - INTERVAL '12 months' as start_date
),
series_dates AS (
    SELECT 
        series_code,
        series_name,
        LAG(CAST(NULLIF(value, '.') as float), -2) OVER (PARTITION BY series_code ORDER BY date DESC) as previous_date,
        LAG(CAST(NULLIF(value, '.') as float), -3) OVER (PARTITION BY series_code ORDER BY date DESC) as two_events_ago
    FROM econ_md.fred_data, date_bounds d
    WHERE fred_data.date >= d.start_date AND fred_data.date <= d.end_date
),
 date_grain as (
  SELECT 
      s.series_code,
      s.series_name,
      COUNT(*) as entry_count,
      CASE 
          WHEN COUNT(*) >= 200 THEN 'Daily'
          WHEN COUNT(*) >= 50 THEN 'Weekly'
          WHEN COUNT(*) >= 9 THEN 'Monthly'
          WHEN COUNT(*) >= 2 THEN 'Quarterly'
          WHEN COUNT(*) >= 1 THEN 'Annually'
          ELSE 'Limited Data' 
      END as date_grain
  FROM 
      series_dates s
  GROUP BY s.series_code,
      s.series_name
  ORDER BY 
      entry_count DESC
),
aggregates as (
  select
    DATE_TRUNC('month', fred_data.date) as month,
    fred_data.series_code,
    fred_data.series_name,
    date_grain.date_grain,
    ROUND(AVG(CAST(NULLIF(fred_data.value, '.') as float)), 4) as clean_value
  FROM econ_md.fred_data
  LEFT JOIN date_grain
    on date_grain.series_code = fred_data.series_code
  WHERE date_grain.date_grain in ('Daily', 'Monthly', 'Quarterly', 'Weekly')
  GROUP BY
    DATE_TRUNC('month', fred_data.date),
    fred_data.series_code,
    date_grain.date_grain,
    fred_data.series_name
  ORDER BY DATE_TRUNC('month', fred_data.date) DESC
),

date_ranges AS (
  SELECT 
      series_code,
      series_name,
      date_grain,
      month,
      clean_value,
      LAG(clean_value, 3) OVER (
          PARTITION BY series_code 
          ORDER BY month
      ) as value_3m_ago,
      LAG(clean_value, 6) OVER (
          PARTITION BY series_code 
          ORDER BY month
      ) as value_6m_ago,
      LAG(clean_value, 12) OVER (
          PARTITION BY series_code 
          ORDER BY month
      ) as value_1y_ago
  FROM aggregates
),
calc_view as (
  SELECT 
      series_code,
      series_name,
      date_grain,
      month,
      clean_value as current_value,
      value_3m_ago,
      value_6m_ago,
      value_1y_ago,
      CASE 
          WHEN value_3m_ago IS NULL OR value_3m_ago = 0 THEN NULL
          ELSE ROUND((clean_value - value_3m_ago ) / (value_3m_ago), 2)
      END as pct_change_3m,
      CASE 
          WHEN value_6m_ago IS NULL OR value_6m_ago = 0 THEN NULL
          ELSE ROUND((clean_value - value_3m_ago ) / (value_6m_ago), 2)
      END as pct_change_6m,
      CASE 
          WHEN value_1y_ago IS NULL OR value_1y_ago = 0 THEN NULL
          ELSE ROUND((clean_value - value_3m_ago ) / (value_1y_ago), 2)
      END as pct_change_1y
  FROM date_ranges
),
  
max_date_view as(  
  SELECT
    series_code,
    Max(month) as month,
  
  FROM calc_view
  GROUP BY
    series_code,
),
  
final as (
  SELECT
    calc_view.series_code,
    calc_view.series_name,
    calc_view.month,
    calc_view.current_value,
    calc_view.pct_change_3m,
    calc_view.pct_change_6m,
    calc_view.pct_change_1y,
    date_grain.date_grain
  FROM calc_view
  JOIN max_date_view
    on max_date_view.series_code = calc_view.series_code
    AND max_date_view.month = calc_view.month
  LEFT JOIN date_grain
    on date_grain.series_code = calc_view.series_code

)

SELECT * FROM final