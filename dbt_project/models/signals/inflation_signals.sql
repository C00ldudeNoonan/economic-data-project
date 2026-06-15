{{ config(
    description='Inflation signals: CPI momentum, Core PCE vs target, breakeven spread'
) }}

/*
    Inflation Signals Model

    Calculates inflation-related signals from FRED data:
    - CPI Momentum: 3-month annualized rate vs 12-month YoY (acceleration/deceleration)
    - Core PCE vs Target: Distance from Fed's 2% target
    - Breakeven Inflation Spread: 5Y vs 10Y breakeven rates

    All source data already ingested via FRED.
*/

WITH cpi_data AS (
    SELECT
        date,
        value AS cpi_level
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'CPIAUCSL'
        AND literal IS NOT NULL
    ORDER BY date
),

core_pce_data AS (
    SELECT
        date,
        value AS pce_level
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'PCEPILFE'
        AND literal IS NOT NULL
    ORDER BY date
),

breakeven_5y AS (
    SELECT
        date,
        literal AS be_5y
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'T5YIE'
        AND literal IS NOT NULL
),

breakeven_10y AS (
    SELECT
        date,
        literal AS be_10y
    FROM {{ ref('stg_fred_series') }}
    WHERE
        series_code = 'T10YIE'
        AND literal IS NOT NULL
),

-- CPI momentum: compare 3-month annualized vs 12-month YoY
cpi_with_changes AS (
    SELECT
        date,
        cpi_level,
        LAG(cpi_level, 3) OVER (ORDER BY date) AS cpi_3m_ago,
        LAG(cpi_level, 12) OVER (ORDER BY date) AS cpi_12m_ago
    FROM cpi_data
),

cpi_momentum AS (
    SELECT
        date,
        cpi_level,
        ROUND(((cpi_level / NULLIF(cpi_3m_ago, 0)) - 1) * 400, 2) AS cpi_3m_annualized,
        ROUND(((cpi_level / NULLIF(cpi_12m_ago, 0)) - 1) * 100, 2) AS cpi_12m_yoy
    FROM cpi_with_changes
    WHERE cpi_3m_ago IS NOT NULL AND cpi_12m_ago IS NOT NULL
),

-- Core PCE YoY
pce_with_changes AS (
    SELECT
        date,
        pce_level,
        LAG(pce_level, 12) OVER (ORDER BY date) AS pce_12m_ago
    FROM core_pce_data
),

pce_yoy AS (
    SELECT
        date,
        pce_level,
        ROUND(((pce_level / NULLIF(pce_12m_ago, 0)) - 1) * 100, 2) AS core_pce_yoy
    FROM pce_with_changes
    WHERE pce_12m_ago IS NOT NULL
),

-- Breakeven spread
breakeven_spread AS (
    SELECT
        b5.date,
        b5.be_5y,
        b10.be_10y,
        ROUND(b5.be_5y - b10.be_10y, 3) AS breakeven_5y_10y_spread
    FROM breakeven_5y AS b5
    INNER JOIN breakeven_10y AS b10 ON b5.date = b10.date
),

-- Combine all inflation signals
combined AS (
    SELECT
        COALESCE(cm.date, py.date, bs.date) AS date,
        cm.cpi_3m_annualized,
        cm.cpi_12m_yoy,
        ROUND(cm.cpi_3m_annualized - cm.cpi_12m_yoy, 2) AS cpi_momentum_spread,
        py.core_pce_yoy,
        ROUND(py.core_pce_yoy - 2.0, 2) AS pce_deviation_from_target,
        bs.be_5y AS breakeven_5y,
        bs.be_10y AS breakeven_10y,
        bs.breakeven_5y_10y_spread
    FROM cpi_momentum AS cm
    FULL OUTER JOIN pce_yoy AS py ON cm.date = py.date
    FULL OUTER JOIN breakeven_spread AS bs ON COALESCE(cm.date, py.date) = bs.date
)

SELECT
    date,
    cpi_3m_annualized,
    cpi_12m_yoy,
    cpi_momentum_spread,
    core_pce_yoy,
    pce_deviation_from_target,
    breakeven_5y,
    breakeven_10y,
    breakeven_5y_10y_spread,

    -- Signal status for CPI momentum
    CASE
        WHEN cpi_momentum_spread > 0.5 THEN 'high'
        WHEN cpi_momentum_spread > 0.0 THEN 'medium'
        WHEN cpi_momentum_spread < -0.5 THEN 'low'
        ELSE 'normal'
    END AS cpi_momentum_status,

    -- Signal status for Core PCE vs target
    CASE
        WHEN core_pce_yoy > 3.5 OR core_pce_yoy < 1.0 THEN 'high'
        WHEN core_pce_yoy > 2.5 OR core_pce_yoy < 1.5 THEN 'medium'
        ELSE 'normal'
    END AS core_pce_status,

    -- Signal status for breakeven spread
    CASE
        WHEN breakeven_5y_10y_spread < -0.5 THEN 'high'
        WHEN breakeven_5y_10y_spread > 0.5 THEN 'medium'
        WHEN ABS(breakeven_5y_10y_spread) > 0.3 THEN 'low'
        ELSE 'normal'
    END AS breakeven_status

FROM combined
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
ORDER BY date DESC
