{{
  config(
    description='Aggregates sector sensitivities into macro factor categories for portfolio exposure scoring'
  )
}}

-- Define macro factor groupings based on indicator characteristics
-- More granular than the simple Growth/Inflation categories
WITH factor_mapping AS (
    SELECT factor_mapping.*
    FROM (VALUES
        -- Inflation factors
        ('CPIAUCSL', 'Inflation', 'Core Inflation'),
        ('CPILFESL', 'Inflation', 'Core Inflation'),
        ('CPIAUCNS', 'Inflation', 'Core Inflation'),
        ('CPILFENS', 'Inflation', 'Core Inflation'),
        ('PCEPI', 'Inflation', 'Core Inflation'),
        ('PCEPILFE', 'Inflation', 'Core Inflation'),
        ('MEDCPIM158SFRBCLE', 'Inflation', 'Core Inflation'),
        ('CORESTICKM159SFRBATL', 'Inflation', 'Sticky Inflation'),
        ('STICKCPIM159SFRBATL', 'Inflation', 'Sticky Inflation'),
        ('PCETRIM12M159SFRBDAL', 'Inflation', 'Core Inflation'),
        ('T10YIE', 'Inflation', 'Inflation Expectations'),
        ('T5YIE', 'Inflation', 'Inflation Expectations'),
        ('T5YIFR', 'Inflation', 'Inflation Expectations'),
        ('PPIACO', 'Inflation', 'Producer Prices'),
        ('PPIFIS', 'Inflation', 'Producer Prices'),
        ('PPIFID', 'Inflation', 'Producer Prices'),
        ('CPIENGSL', 'Inflation', 'Energy Inflation'),

        -- Employment factors
        ('PAYEMS', 'Employment', 'Jobs'),
        ('UNRATE', 'Employment', 'Unemployment'),
        ('U6RATE', 'Employment', 'Unemployment'),
        ('ICSA', 'Employment', 'Unemployment Claims'),
        ('ICSA4WMA', 'Employment', 'Unemployment Claims'),
        ('JTSJOL', 'Employment', 'Job Openings'),
        ('JTSQUR', 'Employment', 'Job Turnover'),
        ('CIVPART', 'Employment', 'Labor Participation'),
        ('EMRATIO', 'Employment', 'Labor Participation'),
        ('CE16OV', 'Employment', 'Jobs'),
        ('MANEMP', 'Employment', 'Manufacturing Jobs'),
        ('USCONS', 'Employment', 'Construction Jobs'),
        ('AHETPI', 'Employment', 'Wages'),
        ('ECIWAG', 'Employment', 'Wages'),

        -- Growth factors
        ('GDP', 'Growth', 'GDP'),
        ('GDPC1', 'Growth', 'GDP'),
        ('GDPC96', 'Growth', 'GDP'),
        ('A191RL1Q225SBEA', 'Growth', 'GDP'),
        ('INDPRO', 'Growth', 'Industrial Production'),
        ('IPMAN', 'Growth', 'Industrial Production'),
        ('TCU', 'Growth', 'Capacity Utilization'),
        ('CAPUTLG2211S', 'Growth', 'Capacity Utilization'),
        ('RSXFS', 'Growth', 'Retail Sales'),
        ('RRSFS', 'Growth', 'Retail Sales'),
        ('PCE', 'Growth', 'Consumer Spending'),
        ('PCEC96', 'Growth', 'Consumer Spending'),
        ('CFNAI', 'Growth', 'Economic Activity'),
        ('CFNAIMA3', 'Growth', 'Economic Activity'),
        ('USSLIND', 'Growth', 'Leading Indicators'),

        -- Housing factors
        ('HOUST', 'Housing', 'Housing Starts'),
        ('HOUST1F', 'Housing', 'Housing Starts'),
        ('PERMIT', 'Housing', 'Building Permits'),
        ('NHSDPTS', 'Housing', 'Home Sales'),
        ('EXHOSLUSM495S', 'Housing', 'Home Sales'),
        ('CSUSHPISA', 'Housing', 'Home Prices'),
        ('MSPUS', 'Housing', 'Home Prices'),
        ('MORTGAGE30US', 'Housing', 'Mortgage Rates'),
        ('MORTGAGE15US', 'Housing', 'Mortgage Rates'),

        -- Consumer Sentiment factors
        ('UMCSENT', 'Consumer', 'Consumer Sentiment'),
        ('CSCICP03USM665S', 'Consumer', 'Consumer Confidence'),
        ('PSAVERT', 'Consumer', 'Savings Rate'),
        ('DSPIC96', 'Consumer', 'Income'),
        ('PI', 'Consumer', 'Income'),

        -- Financial Conditions factors
        ('DFF', 'Rates', 'Fed Funds'),
        ('FEDFUNDS', 'Rates', 'Fed Funds'),
        ('DGS10', 'Rates', 'Treasury Yields'),
        ('TB10YR', 'Rates', 'Treasury Yields'),
        ('TB2YR', 'Rates', 'Treasury Yields'),
        ('T10Y2Y', 'Rates', 'Yield Curve'),
        ('T10Y3M', 'Rates', 'Yield Curve'),
        ('VIXCLS', 'Financial', 'Volatility'),
        ('NFCI', 'Financial', 'Financial Conditions'),
        ('NFCICREDIT', 'Financial', 'Credit Conditions'),
        ('BAMLC0A0CM', 'Financial', 'Credit Spreads'),
        ('BAMLH0A0HYM2', 'Financial', 'Credit Spreads'),
        ('TEDRATE', 'Financial', 'Credit Spreads'),

        -- Business Activity factors
        ('IPMAN', 'Business', 'Manufacturing Production'),
        ('NEWORDER', 'Business', 'Manufacturing Orders'),
        ('MANEMP', 'Business', 'Manufacturing Employment'),
        ('BPEA', 'Business', 'Business Outlook'),
        ('GACDISA066MSFRBNY', 'Business', 'Regional Surveys')
    ) AS factor_mapping (series_code, macro_factor, sub_factor)
),

-- Get sector sensitivity scores with factor mapping
sector_factor_sensitivity AS (
    SELECT
        sis.symbol,
        sis.sector_name,
        fm.macro_factor,
        fm.sub_factor,
        sis.series_code,
        sis.sensitivity_score,
        sis.corr_1mo_contemp,
        sis.corr_3mo_contemp,
        sis.best_lag_correlation_abs
    FROM {{ ref('sector_indicator_sensitivity') }} sis
    INNER JOIN factor_mapping fm ON sis.series_code = fm.series_code
    WHERE sis.sensitivity_score IS NOT NULL
),

-- Aggregate by sector and macro factor
sector_factor_scores AS (
    SELECT
        symbol,
        sector_name,
        macro_factor,
        COUNT(DISTINCT series_code) AS indicator_count,
        ROUND(AVG(sensitivity_score), 2) AS avg_sensitivity,
        ROUND(MAX(sensitivity_score), 2) AS max_sensitivity,
        ROUND(AVG(ABS(corr_1mo_contemp)), 4) AS avg_abs_correlation,
        ROUND(AVG(best_lag_correlation_abs), 4) AS avg_lag_correlation,
        -- Factor exposure score (0-100)
        ROUND(
            (AVG(sensitivity_score) * 0.6 +
             AVG(best_lag_correlation_abs) * 100 * 0.4),
            2
        ) AS factor_exposure_score,
        -- List top indicators for this factor
        STRING_AGG(DISTINCT sub_factor, ', ' ORDER BY sub_factor) AS sub_factors
    FROM sector_factor_sensitivity
    GROUP BY symbol, sector_name, macro_factor
),

-- Calculate sector-level aggregate exposure
sector_aggregate AS (
    SELECT
        symbol,
        sector_name,
        SUM(indicator_count) AS total_indicators,
        ROUND(AVG(factor_exposure_score), 2) AS overall_macro_exposure,
        -- Breakdown by factor
        MAX(CASE WHEN macro_factor = 'Inflation' THEN factor_exposure_score END) AS inflation_exposure,
        MAX(CASE WHEN macro_factor = 'Employment' THEN factor_exposure_score END) AS employment_exposure,
        MAX(CASE WHEN macro_factor = 'Growth' THEN factor_exposure_score END) AS growth_exposure,
        MAX(CASE WHEN macro_factor = 'Housing' THEN factor_exposure_score END) AS housing_exposure,
        MAX(CASE WHEN macro_factor = 'Consumer' THEN factor_exposure_score END) AS consumer_exposure,
        MAX(CASE WHEN macro_factor = 'Rates' THEN factor_exposure_score END) AS rates_exposure,
        MAX(CASE WHEN macro_factor = 'Financial' THEN factor_exposure_score END) AS financial_exposure,
        MAX(CASE WHEN macro_factor = 'Business' THEN factor_exposure_score END) AS business_exposure
    FROM sector_factor_scores
    GROUP BY symbol, sector_name
)

SELECT
    sfs.symbol,
    sfs.sector_name,
    sfs.macro_factor,
    sfs.indicator_count,
    sfs.avg_sensitivity,
    sfs.max_sensitivity,
    sfs.avg_abs_correlation,
    sfs.avg_lag_correlation,
    sfs.factor_exposure_score,
    sfs.sub_factors,
    -- Join aggregate scores
    sa.overall_macro_exposure,
    sa.inflation_exposure,
    sa.employment_exposure,
    sa.growth_exposure,
    sa.housing_exposure,
    sa.consumer_exposure,
    sa.rates_exposure,
    sa.financial_exposure,
    sa.business_exposure,
    -- Rank sectors by factor exposure
    RANK() OVER (
        PARTITION BY macro_factor
        ORDER BY factor_exposure_score DESC
    ) AS factor_rank
FROM sector_factor_scores sfs
INNER JOIN sector_aggregate sa ON sfs.symbol = sa.symbol
ORDER BY sfs.symbol, sfs.factor_exposure_score DESC
