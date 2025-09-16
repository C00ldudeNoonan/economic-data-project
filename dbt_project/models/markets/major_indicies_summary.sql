{{ config(
    materialized='view'
) }}


WITH base_data AS (
    SELECT 
        symbol,
        asset_type,
        CAST(date AS DATE) as trade_date,
        open,
        close,
        adj_open,
        adj_close,
        exchange,
        name,
        -- Calculate price changes
        (close - open) as price_change_raw,
        (adj_close - adj_open) as price_change_adj,
        -- Calculate percentage changes
        CASE 
            WHEN open > 0 THEN ((close - open) / open) * 100 
            ELSE NULL 
        END as pct_change_raw,
        CASE 
            WHEN adj_open > 0 THEN ((adj_close - adj_open) / adj_open) * 100 
            ELSE NULL 
        END as pct_change_adj
    FROM {{ ref('stg_major_indices') }}
    WHERE date IS NOT NULL 
        AND open IS NOT NULL 
        AND close IS NOT NULL
        AND open > 0
),

-- Define date boundaries for different periods
date_boundaries AS (
    SELECT 
        CURRENT_DATE as today,
        CURRENT_DATE - INTERVAL '12 weeks' as twelve_weeks_ago,
        CURRENT_DATE - INTERVAL '6 months' as six_months_ago,  
        CURRENT_DATE - INTERVAL '1 year' as one_year_ago,
        CURRENT_DATE - INTERVAL '5 years' as five_years_ago
),

-- Filter data for each time period
filtered_data AS (
    SELECT 
        bd.*,
        CASE 
            WHEN bd.trade_date >= db.twelve_weeks_ago THEN '12_weeks'
            WHEN bd.trade_date >= db.six_months_ago THEN '6_months' 
            WHEN bd.trade_date >= db.one_year_ago THEN '1_year'
            WHEN bd.trade_date >= db.five_years_ago THEN '5_years'
            ELSE 'older'
        END as time_period
    FROM base_data bd
    CROSS JOIN date_boundaries db
    WHERE bd.trade_date >= db.five_years_ago  -- Only include data within 5 years
),

-- Get first and last prices for each period (separate from main aggregation)
period_boundaries AS (
    SELECT 
        symbol,
        asset_type,
        time_period,
        MIN(trade_date) as period_start_date,
        MAX(trade_date) as period_end_date
    FROM filtered_data
    WHERE time_period != 'older'
    GROUP BY symbol, asset_type, time_period
),

-- Get start and end prices
start_prices AS (
    SELECT 
        pb.symbol,
        pb.asset_type,
        pb.time_period,
        fd.adj_open as period_start_price
    FROM period_boundaries pb
    JOIN filtered_data fd ON 
        pb.symbol = fd.symbol 
        AND pb.asset_type = fd.asset_type
        AND pb.time_period = fd.time_period
        AND pb.period_start_date = fd.trade_date
),

end_prices AS (
    SELECT 
        pb.symbol,
        pb.asset_type,
        pb.time_period,
        fd.adj_close as period_end_price
    FROM period_boundaries pb
    JOIN filtered_data fd ON 
        pb.symbol = fd.symbol 
        AND pb.asset_type = fd.asset_type
        AND pb.time_period = fd.time_period
        AND pb.period_end_date = fd.trade_date
),

-- Main aggregation without window functions
aggregated_results AS (
    SELECT 
        symbol,
        asset_type,
        time_period,
        exchange,
        name,
        
        -- Date range info
        MIN(trade_date) as period_start_date,
        MAX(trade_date) as period_end_date,
        COUNT(*) as trading_days,
        
        -- Raw price change metrics
        SUM(price_change_raw) as total_price_change_raw,
        AVG(price_change_raw) as avg_daily_price_change_raw,
        STDDEV(price_change_raw) as stddev_price_change_raw,
        MIN(price_change_raw) as min_daily_change_raw,
        MAX(price_change_raw) as max_daily_change_raw,
        
        -- Adjusted price change metrics  
        SUM(price_change_adj) as total_price_change_adj,
        AVG(price_change_adj) as avg_daily_price_change_adj,
        STDDEV(price_change_adj) as stddev_price_change_adj,
        MIN(price_change_adj) as min_daily_change_adj,
        MAX(price_change_adj) as max_daily_change_adj,
        
        -- Percentage change metrics
        AVG(pct_change_raw) as avg_daily_pct_change_raw,
        STDDEV(pct_change_raw) as stddev_pct_change_raw,
        MIN(pct_change_raw) as min_daily_pct_change_raw,
        MAX(pct_change_raw) as max_daily_pct_change_raw,
        
        -- Adjusted percentage change metrics
        AVG(pct_change_adj) as avg_daily_pct_change_adj,
        STDDEV(pct_change_adj) as stddev_pct_change_adj,
        MIN(pct_change_adj) as min_daily_pct_change_adj,
        MAX(pct_change_adj) as max_daily_pct_change_adj,
        
        -- Win/loss days
        SUM(CASE WHEN price_change_adj > 0 THEN 1 ELSE 0 END) as positive_days,
        SUM(CASE WHEN price_change_adj < 0 THEN 1 ELSE 0 END) as negative_days,
        SUM(CASE WHEN price_change_adj = 0 THEN 1 ELSE 0 END) as neutral_days
        
    FROM filtered_data
    WHERE time_period != 'older'
    GROUP BY 
        symbol, 
        asset_type, 
        time_period, 
        exchange, 
        name
),

-- Combine aggregated results with period boundary prices
combined_results AS (
    SELECT 
        ar.*,
        sp.period_start_price,
        ep.period_end_price
    FROM aggregated_results ar
    LEFT JOIN start_prices sp ON 
        ar.symbol = sp.symbol 
        AND ar.asset_type = sp.asset_type 
        AND ar.time_period = sp.time_period
    LEFT JOIN end_prices ep ON 
        ar.symbol = ep.symbol 
        AND ar.asset_type = ep.asset_type 
        AND ar.time_period = ep.time_period
),

-- Calculate final metrics
final_metrics AS (
    SELECT *,
        -- Total period return
        CASE 
            WHEN period_start_price > 0 THEN 
                ((period_end_price - period_start_price) / period_start_price) * 100
            ELSE NULL 
        END as total_period_return_pct,
        
        -- Win rate
        CASE 
            WHEN trading_days > 0 THEN 
                (positive_days * 100.0) / trading_days 
            ELSE NULL 
        END as win_rate_pct,
        
        -- Annualized volatility 
        stddev_pct_change_adj * SQRT(252) as annualized_volatility_pct
        
    FROM combined_results
)

-- Final results
SELECT 
    symbol,
    asset_type,
    time_period,
    exchange,
    name,
    period_start_date,
    period_end_date,
    trading_days,
    
    -- Key performance metrics
    ROUND(total_period_return_pct, 2) as total_return_pct,
    ROUND(avg_daily_pct_change_adj, 4) as avg_daily_return_pct,
    ROUND(annualized_volatility_pct, 2) as volatility_pct,
    ROUND(win_rate_pct, 1) as win_rate_pct,
    
    -- Price change details
    ROUND(total_price_change_adj, 2) as total_price_change,
    ROUND(avg_daily_price_change_adj, 4) as avg_daily_price_change,
    ROUND(min_daily_change_adj, 2) as worst_day_change,
    ROUND(max_daily_change_adj, 2) as best_day_change,
    
    -- Trading activity
    positive_days,
    negative_days,
    neutral_days,
    
    -- Period prices
    ROUND(period_start_price, 2) as period_start_price,
    ROUND(period_end_price, 2) as period_end_price

FROM final_metrics
ORDER BY 
    time_period,
    asset_type,
    symbol