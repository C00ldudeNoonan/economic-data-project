{{
  config(
    description='Aggregates sector sensitivities into macro factor categories for portfolio exposure scoring'
  )
}}

-- Define macro factor groupings based on indicator characteristics
-- More granular than the simple Growth/Inflation categories
WITH factor_mapping AS (
    SELECT *
    FROM UNNEST([
        -- Inflation factors
        STRUCT('CPIAUCSL' AS series_code, 'Inflation' AS macro_factor, 'Core Inflation' AS sub_factor),
        STRUCT('CPILFESL' AS series_code, 'Inflation' AS macro_factor, 'Core Inflation' AS sub_factor),
        STRUCT('CPIAUCNS' AS series_code, 'Inflation' AS macro_factor, 'Core Inflation' AS sub_factor),
        STRUCT('CPILFENS' AS series_code, 'Inflation' AS macro_factor, 'Core Inflation' AS sub_factor),
        STRUCT('PCEPI' AS series_code, 'Inflation' AS macro_factor, 'Core Inflation' AS sub_factor),
        STRUCT('PCEPILFE' AS series_code, 'Inflation' AS macro_factor, 'Core Inflation' AS sub_factor),
        STRUCT('MEDCPIM158SFRBCLE' AS series_code, 'Inflation' AS macro_factor, 'Core Inflation' AS sub_factor),
        STRUCT('CORESTICKM159SFRBATL' AS series_code, 'Inflation' AS macro_factor, 'Sticky Inflation' AS sub_factor),
        STRUCT('STICKCPIM159SFRBATL' AS series_code, 'Inflation' AS macro_factor, 'Sticky Inflation' AS sub_factor),
        STRUCT('PCETRIM12M159SFRBDAL' AS series_code, 'Inflation' AS macro_factor, 'Core Inflation' AS sub_factor),
        STRUCT('T10YIE' AS series_code, 'Inflation' AS macro_factor, 'Inflation Expectations' AS sub_factor),
        STRUCT('T5YIE' AS series_code, 'Inflation' AS macro_factor, 'Inflation Expectations' AS sub_factor),
        STRUCT('T5YIFR' AS series_code, 'Inflation' AS macro_factor, 'Inflation Expectations' AS sub_factor),
        STRUCT('PPIACO' AS series_code, 'Inflation' AS macro_factor, 'Producer Prices' AS sub_factor),
        STRUCT('PPIFIS' AS series_code, 'Inflation' AS macro_factor, 'Producer Prices' AS sub_factor),
        STRUCT('PPIFID' AS series_code, 'Inflation' AS macro_factor, 'Producer Prices' AS sub_factor),
        STRUCT('CPIENGSL' AS series_code, 'Inflation' AS macro_factor, 'Energy Inflation' AS sub_factor),

        -- Employment factors
        STRUCT('PAYEMS' AS series_code, 'Employment' AS macro_factor, 'Jobs' AS sub_factor),
        STRUCT('UNRATE' AS series_code, 'Employment' AS macro_factor, 'Unemployment' AS sub_factor),
        STRUCT('U6RATE' AS series_code, 'Employment' AS macro_factor, 'Unemployment' AS sub_factor),
        STRUCT('ICSA' AS series_code, 'Employment' AS macro_factor, 'Unemployment Claims' AS sub_factor),
        STRUCT('ICSA4WMA' AS series_code, 'Employment' AS macro_factor, 'Unemployment Claims' AS sub_factor),
        STRUCT('JTSJOL' AS series_code, 'Employment' AS macro_factor, 'Job Openings' AS sub_factor),
        STRUCT('JTSQUR' AS series_code, 'Employment' AS macro_factor, 'Job Turnover' AS sub_factor),
        STRUCT('CIVPART' AS series_code, 'Employment' AS macro_factor, 'Labor Participation' AS sub_factor),
        STRUCT('EMRATIO' AS series_code, 'Employment' AS macro_factor, 'Labor Participation' AS sub_factor),
        STRUCT('CE16OV' AS series_code, 'Employment' AS macro_factor, 'Jobs' AS sub_factor),
        STRUCT('MANEMP' AS series_code, 'Employment' AS macro_factor, 'Manufacturing Jobs' AS sub_factor),
        STRUCT('USCONS' AS series_code, 'Employment' AS macro_factor, 'Construction Jobs' AS sub_factor),
        STRUCT('AHETPI' AS series_code, 'Employment' AS macro_factor, 'Wages' AS sub_factor),
        STRUCT('ECIWAG' AS series_code, 'Employment' AS macro_factor, 'Wages' AS sub_factor),

        -- Growth factors
        STRUCT('GDP' AS series_code, 'Growth' AS macro_factor, 'GDP' AS sub_factor),
        STRUCT('GDPC1' AS series_code, 'Growth' AS macro_factor, 'GDP' AS sub_factor),
        STRUCT('GDPC96' AS series_code, 'Growth' AS macro_factor, 'GDP' AS sub_factor),
        STRUCT('A191RL1Q225SBEA' AS series_code, 'Growth' AS macro_factor, 'GDP' AS sub_factor),
        STRUCT('INDPRO' AS series_code, 'Growth' AS macro_factor, 'Industrial Production' AS sub_factor),
        STRUCT('IPMAN' AS series_code, 'Growth' AS macro_factor, 'Industrial Production' AS sub_factor),
        STRUCT('TCU' AS series_code, 'Growth' AS macro_factor, 'Capacity Utilization' AS sub_factor),
        STRUCT('CAPUTLG2211S' AS series_code, 'Growth' AS macro_factor, 'Capacity Utilization' AS sub_factor),
        STRUCT('RSXFS' AS series_code, 'Growth' AS macro_factor, 'Retail Sales' AS sub_factor),
        STRUCT('RRSFS' AS series_code, 'Growth' AS macro_factor, 'Retail Sales' AS sub_factor),
        STRUCT('PCE' AS series_code, 'Growth' AS macro_factor, 'Consumer Spending' AS sub_factor),
        STRUCT('PCEC96' AS series_code, 'Growth' AS macro_factor, 'Consumer Spending' AS sub_factor),
        STRUCT('CFNAI' AS series_code, 'Growth' AS macro_factor, 'Economic Activity' AS sub_factor),
        STRUCT('CFNAIMA3' AS series_code, 'Growth' AS macro_factor, 'Economic Activity' AS sub_factor),
        STRUCT('USSLIND' AS series_code, 'Growth' AS macro_factor, 'Leading Indicators' AS sub_factor),

        -- Housing factors
        STRUCT('HOUST' AS series_code, 'Housing' AS macro_factor, 'Housing Starts' AS sub_factor),
        STRUCT('HOUST1F' AS series_code, 'Housing' AS macro_factor, 'Housing Starts' AS sub_factor),
        STRUCT('PERMIT' AS series_code, 'Housing' AS macro_factor, 'Building Permits' AS sub_factor),
        STRUCT('NHSDPTS' AS series_code, 'Housing' AS macro_factor, 'Home Sales' AS sub_factor),
        STRUCT('EXHOSLUSM495S' AS series_code, 'Housing' AS macro_factor, 'Home Sales' AS sub_factor),
        STRUCT('CSUSHPISA' AS series_code, 'Housing' AS macro_factor, 'Home Prices' AS sub_factor),
        STRUCT('MSPUS' AS series_code, 'Housing' AS macro_factor, 'Home Prices' AS sub_factor),
        STRUCT('MORTGAGE30US' AS series_code, 'Housing' AS macro_factor, 'Mortgage Rates' AS sub_factor),
        STRUCT('MORTGAGE15US' AS series_code, 'Housing' AS macro_factor, 'Mortgage Rates' AS sub_factor),

        -- Consumer Sentiment factors
        STRUCT('UMCSENT' AS series_code, 'Consumer' AS macro_factor, 'Consumer Sentiment' AS sub_factor),
        STRUCT('CSCICP03USM665S' AS series_code, 'Consumer' AS macro_factor, 'Consumer Confidence' AS sub_factor),
        STRUCT('PSAVERT' AS series_code, 'Consumer' AS macro_factor, 'Savings Rate' AS sub_factor),
        STRUCT('DSPIC96' AS series_code, 'Consumer' AS macro_factor, 'Income' AS sub_factor),
        STRUCT('PI' AS series_code, 'Consumer' AS macro_factor, 'Income' AS sub_factor),

        -- Financial Conditions factors
        STRUCT('DFF' AS series_code, 'Rates' AS macro_factor, 'Fed Funds' AS sub_factor),
        STRUCT('FEDFUNDS' AS series_code, 'Rates' AS macro_factor, 'Fed Funds' AS sub_factor),
        STRUCT('DGS10' AS series_code, 'Rates' AS macro_factor, 'Treasury Yields' AS sub_factor),
        STRUCT('TB10YR' AS series_code, 'Rates' AS macro_factor, 'Treasury Yields' AS sub_factor),
        STRUCT('TB2YR' AS series_code, 'Rates' AS macro_factor, 'Treasury Yields' AS sub_factor),
        STRUCT('T10Y2Y' AS series_code, 'Rates' AS macro_factor, 'Yield Curve' AS sub_factor),
        STRUCT('T10Y3M' AS series_code, 'Rates' AS macro_factor, 'Yield Curve' AS sub_factor),
        STRUCT('VIXCLS' AS series_code, 'Financial' AS macro_factor, 'Volatility' AS sub_factor),
        STRUCT('NFCI' AS series_code, 'Financial' AS macro_factor, 'Financial Conditions' AS sub_factor),
        STRUCT('NFCICREDIT' AS series_code, 'Financial' AS macro_factor, 'Credit Conditions' AS sub_factor),
        STRUCT('BAMLC0A0CM' AS series_code, 'Financial' AS macro_factor, 'Credit Spreads' AS sub_factor),
        STRUCT('BAMLH0A0HYM2' AS series_code, 'Financial' AS macro_factor, 'Credit Spreads' AS sub_factor),
        STRUCT('TEDRATE' AS series_code, 'Financial' AS macro_factor, 'Credit Spreads' AS sub_factor),

        -- Business Activity factors
        STRUCT('IPMAN' AS series_code, 'Business' AS macro_factor, 'Manufacturing Production' AS sub_factor),
        STRUCT('NEWORDER' AS series_code, 'Business' AS macro_factor, 'Manufacturing Orders' AS sub_factor),
        STRUCT('MANEMP' AS series_code, 'Business' AS macro_factor, 'Manufacturing Employment' AS sub_factor),
        STRUCT('BPEA' AS series_code, 'Business' AS macro_factor, 'Business Outlook' AS sub_factor),
        STRUCT('GACDISA066MSFRBNY' AS series_code, 'Business' AS macro_factor, 'Regional Surveys' AS sub_factor)
    ])
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
