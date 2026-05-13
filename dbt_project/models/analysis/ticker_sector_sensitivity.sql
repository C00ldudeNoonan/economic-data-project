{{
  config(
    description='Maps individual stock tickers to sectors and their economic indicator sensitivity for watchlist analysis'
  )
}}

-- Ticker-Sector Sensitivity Mapping
-- Enables watchlist impact analysis by connecting individual stocks to macro indicators via sector proxy

-- Mapping from GICS sector names to sector ETF symbols
WITH sector_etf_mapping AS (
    SELECT sector_etf_mapping.*
    FROM (VALUES
        ('Information Technology', 'XLK', 'Technology'),
        ('Communication Services', 'XLC', 'Communication Services'),
        ('Consumer Discretionary', 'XLY', 'Consumer Discretionary'),
        ('Financials', 'XLF', 'Financial'),
        ('Industrials', 'XLI', 'Industrial'),
        ('Utilities', 'XLU', 'Utilities'),
        ('Consumer Staples', 'XLP', 'Consumer Staples'),
        ('Real Estate', 'XLRE', 'Real Estate'),
        ('Materials', 'XLB', 'Materials'),
        ('Energy', 'XLE', 'Energy'),
        ('Health Care', 'XLV', 'Health Care')
    ) AS sector_etf_mapping (gics_sector, etf_symbol, sector_display_name)
),

-- Get S&P 500 company metadata with sector info
sp500_companies AS (
    SELECT DISTINCT
        symbol,
        company_name,
        sector AS gics_sector,
        sub_industry,
        'SP500' AS index_membership
    FROM {{ ref('stg_sp500_companies_active') }}
    WHERE symbol IS NOT NULL
),

-- Join companies to sector ETFs
ticker_sector_mapping AS (
    SELECT
        c.symbol,
        c.company_name,
        c.gics_sector,
        c.sub_industry,
        c.index_membership,
        sem.etf_symbol AS sector_etf,
        sem.sector_display_name
    FROM sp500_companies c
    LEFT JOIN sector_etf_mapping sem ON c.gics_sector = sem.gics_sector
),

-- Get top indicators per sector from sensitivity summary
sector_top_indicators AS (
    SELECT
        symbol AS sector_etf,
        series_code,
        series_name,
        indicator_category,
        sensitivity_score,
        corr_1mo_contemp AS correlation,
        has_predictive_power,
        rank_in_sector
    FROM {{ ref('sector_sensitivity_summary') }}
    WHERE rank_in_sector <= 5  -- Top 5 indicators per sector
),

-- Get sector regime performance summary
sector_regime_summary AS (
    SELECT
        symbol AS sector_etf,
        sector_type,
        MAX(CASE WHEN regime = 'Expansion' THEN avg_monthly_return END) AS expansion_return,
        MAX(CASE WHEN regime = 'Contraction' THEN avg_monthly_return END) AS contraction_return,
        MAX(CASE WHEN regime = 'Slowdown' THEN avg_monthly_return END) AS slowdown_return,
        MAX(CASE WHEN regime = 'Recovery' THEN avg_monthly_return END) AS recovery_return
    FROM {{ ref('sector_regime_performance') }}
    GROUP BY symbol, sector_type
),

-- Calculate sector-level sensitivity summary
sector_sensitivity_agg AS (
    SELECT
        sector_etf,
        COUNT(*) AS n_sensitive_indicators,
        AVG(sensitivity_score) AS avg_sensitivity_score,
        MAX(sensitivity_score) AS max_sensitivity_score,
        STRING_AGG(DISTINCT series_code, ', ' ORDER BY sensitivity_score DESC) AS top_indicator_codes,
        STRING_AGG(DISTINCT series_name, '; ' ORDER BY sensitivity_score DESC) AS top_indicator_names
    FROM sector_top_indicators
    GROUP BY sector_etf
)

SELECT
    tsm.symbol AS ticker,
    tsm.company_name,
    tsm.gics_sector,
    tsm.sub_industry,
    tsm.index_membership,
    tsm.sector_etf,
    tsm.sector_display_name,

    -- Sector sensitivity metrics
    ssa.n_sensitive_indicators,
    ROUND(ssa.avg_sensitivity_score, 2) AS avg_sector_sensitivity,
    ROUND(ssa.max_sensitivity_score, 2) AS max_sector_sensitivity,
    ssa.top_indicator_codes,
    ssa.top_indicator_names,

    -- Sector regime characteristics
    srs.sector_type,
    ROUND(srs.expansion_return, 2) AS expansion_avg_return,
    ROUND(srs.contraction_return, 2) AS contraction_avg_return,
    ROUND(srs.slowdown_return, 2) AS slowdown_avg_return,
    ROUND(srs.recovery_return, 2) AS recovery_avg_return,

    -- Macro exposure classification
    CASE
        WHEN ssa.avg_sensitivity_score >= 20 THEN 'High'
        WHEN ssa.avg_sensitivity_score >= 10 THEN 'Medium'
        ELSE 'Low'
    END AS macro_exposure_level

FROM ticker_sector_mapping tsm
LEFT JOIN sector_sensitivity_agg ssa ON tsm.sector_etf = ssa.sector_etf
LEFT JOIN sector_regime_summary srs ON tsm.sector_etf = srs.sector_etf
WHERE tsm.sector_etf IS NOT NULL

ORDER BY tsm.symbol
