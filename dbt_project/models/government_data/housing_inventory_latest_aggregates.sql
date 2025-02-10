{{ config(
    materialized='table'
) }}


With inventory as (
  SELECT 
    data_type_code as series_code,
    seasonally_adj,
    category_code,
    cast(cell_value as float) as clean_value,
    error_data,
    time,
    series_name,
    plot_groupings,
    CAST((CASE 
        WHEN RIGHT(time, 2) = 'Q1' THEN (LEFT(time, 4) || '-01-01')::DATE
        WHEN RIGHT(time, 2) = 'Q2' THEN (LEFT(time, 4) || '-04-01')::DATE
        WHEN RIGHT(time, 2) = 'Q3' THEN (LEFT(time, 4) || '-07-01')::DATE
        WHEN RIGHT(time, 2) = 'Q4' THEN (LEFT(time, 4) || '-10-01')::DATE
    END) as date) as month,
  'Quarterly' as date_grain
  FROM {{ref('stg_housing_inventory')}}
  WHERE cell_value <> '(z)' AND error_data = 'no'
),
date_ranges AS (
  SELECT 
      series_code,
      series_name,
      date_grain,
      month,
      clean_value,
      LAG(clean_value, 1) OVER (
          PARTITION BY series_code 
          ORDER BY month
      ) as value_3m_ago,
      LAG(clean_value, 2) OVER (
          PARTITION BY series_code 
          ORDER BY month
      ) as value_6m_ago,
      LAG(clean_value, 4) OVER (
          PARTITION BY series_code 
          ORDER BY month
      ) as value_1y_ago
  FROM inventory
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
    calc_view.date_grain
  FROM calc_view
  JOIN max_date_view
    on max_date_view.series_code = calc_view.series_code
    AND max_date_view.month = calc_view.month
)

SELECT * FROM final
