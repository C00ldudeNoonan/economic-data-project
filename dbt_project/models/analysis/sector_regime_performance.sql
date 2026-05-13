{{
  config(
    description='Calculates sector ETF performance during each economic regime for defensive/cyclical analysis'
  )
}}

-- Sector Performance by Economic Regime
-- Identifies which sectors outperform/underperform in each regime

WITH sector_monthly AS (
    -- Get monthly sector returns
    SELECT
        symbol,
        DATE_TRUNC('month', date) AS month_date,
        -- Use the last day's data for each month
        LAST_VALUE(pct_change_1mo) OVER (
            PARTITION BY symbol, DATE_TRUNC('month', date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS monthly_return,
        LAST_VALUE(pct_change_3mo) OVER (
            PARTITION BY symbol, DATE_TRUNC('month', date)
            ORDER BY date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS return_3mo,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, DATE_TRUNC('month', date)
            ORDER BY date DESC
        ) AS rn
    FROM {{ ref('us_sector_analysis_return') }}
    WHERE symbol IN ('XLK', 'XLC', 'XLY', 'XLF', 'XLI', 'XLU', 'XLP', 'XLRE', 'XLB', 'XLE', 'XLV')
),

sector_returns AS (
    SELECT
        symbol,
        month_date,
        monthly_return,
        return_3mo
    FROM sector_monthly
    WHERE rn = 1
),

-- Sector name mapping
sector_names AS (
    SELECT sector_names.*
    FROM (VALUES
        ('XLK', 'Technology', 'Cyclical'),
        ('XLC', 'Communication Services', 'Cyclical'),
        ('XLY', 'Consumer Discretionary', 'Cyclical'),
        ('XLF', 'Financial', 'Cyclical'),
        ('XLI', 'Industrial', 'Cyclical'),
        ('XLU', 'Utilities', 'Defensive'),
        ('XLP', 'Consumer Staples', 'Defensive'),
        ('XLRE', 'Real Estate', 'Interest-Sensitive'),
        ('XLB', 'Materials', 'Cyclical'),
        ('XLE', 'Energy', 'Cyclical'),
        ('XLV', 'Health Care', 'Defensive')
    ) AS sector_names (symbol, sector_name, sector_type)
),

-- Join sector returns with regime classification
sector_regime_data AS (
    SELECT
        sr.symbol,
        sn.sector_name,
        sn.sector_type,
        sr.month_date,
        sr.monthly_return,
        sr.return_3mo,
        rc.regime,
        rc.confidence,
        rc.composite_score
    FROM sector_returns sr
    INNER JOIN {{ ref('economic_regime_classification') }} rc
        ON sr.month_date = rc.month_date
    LEFT JOIN sector_names sn ON sr.symbol = sn.symbol
    WHERE sr.monthly_return IS NOT NULL
),

-- Calculate performance stats by regime for each sector
regime_performance AS (
    SELECT
        symbol,
        sector_name,
        sector_type,
        regime,
        COUNT(*) AS months_in_regime,
        ROUND(AVG(monthly_return), 2) AS avg_monthly_return,
        ROUND(STDDEV(monthly_return), 2) AS return_volatility,
        ROUND(AVG(monthly_return) / NULLIF(STDDEV(monthly_return), 0), 2) AS sharpe_proxy,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY monthly_return), 2) AS median_return,
        ROUND(MIN(monthly_return), 2) AS worst_month,
        ROUND(MAX(monthly_return), 2) AS best_month,
        SUM(CASE WHEN monthly_return > 0 THEN 1 ELSE 0 END) AS positive_months,
        ROUND(SUM(CASE WHEN monthly_return > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate
    FROM sector_regime_data
    GROUP BY symbol, sector_name, sector_type, regime
),

-- Calculate overall sector performance for comparison
overall_performance AS (
    SELECT
        symbol,
        sector_name,
        sector_type,
        'Overall' AS regime,
        COUNT(*) AS months_in_regime,
        ROUND(AVG(monthly_return), 2) AS avg_monthly_return,
        ROUND(STDDEV(monthly_return), 2) AS return_volatility,
        ROUND(AVG(monthly_return) / NULLIF(STDDEV(monthly_return), 0), 2) AS sharpe_proxy,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY monthly_return), 2) AS median_return,
        ROUND(MIN(monthly_return), 2) AS worst_month,
        ROUND(MAX(monthly_return), 2) AS best_month,
        SUM(CASE WHEN monthly_return > 0 THEN 1 ELSE 0 END) AS positive_months,
        ROUND(SUM(CASE WHEN monthly_return > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate
    FROM sector_regime_data
    GROUP BY symbol, sector_name, sector_type
),

-- Calculate regime averages across all sectors for relative performance
regime_averages AS (
    SELECT
        regime,
        ROUND(AVG(avg_monthly_return), 2) AS regime_avg_return
    FROM regime_performance
    GROUP BY regime
),

-- Combine and add relative performance metrics
combined AS (
    SELECT
        rp.*,
        ra.regime_avg_return,
        ROUND(rp.avg_monthly_return - ra.regime_avg_return, 2) AS relative_performance,
        -- Rank sectors within each regime
        ROW_NUMBER() OVER (PARTITION BY rp.regime ORDER BY rp.avg_monthly_return DESC) AS regime_rank

    FROM regime_performance rp
    LEFT JOIN regime_averages ra ON rp.regime = ra.regime

    UNION ALL

    SELECT
        op.*,
        NULL AS regime_avg_return,
        NULL AS relative_performance,
        NULL AS regime_rank
    FROM overall_performance op
)

SELECT
    symbol,
    sector_name,
    sector_type,
    regime,
    months_in_regime,
    avg_monthly_return,
    return_volatility,
    sharpe_proxy,
    median_return,
    worst_month,
    best_month,
    positive_months,
    win_rate,
    regime_avg_return,
    relative_performance,
    regime_rank,

    -- Classification: outperformer/underperformer in regime
    CASE
        WHEN relative_performance >= 0.5 THEN 'Strong Outperformer'
        WHEN relative_performance > 0 THEN 'Outperformer'
        WHEN relative_performance >= -0.5 THEN 'Underperformer'
        ELSE 'Strong Underperformer'
    END AS regime_classification,

    -- Best regime for this sector
    CASE
        WHEN regime_rank = 1 THEN TRUE
        ELSE FALSE
    END AS is_top_performer

FROM combined
ORDER BY
    CASE regime
        WHEN 'Expansion' THEN 1
        WHEN 'Slowdown' THEN 2
        WHEN 'Contraction' THEN 3
        WHEN 'Recovery' THEN 4
        ELSE 5
    END,
    avg_monthly_return DESC
