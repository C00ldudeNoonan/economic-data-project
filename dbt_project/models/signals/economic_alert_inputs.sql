{{ config(
    description='Latest values per indicator referenced by the economic alerts engine'
) }}

/*
    Economic Alert Inputs

    Emits exactly one row per observation date with the latest computed
    values for each indicator referenced by `alerts/definitions.yaml`.
    The Dagster alert evaluation asset selects the most recent row whose
    columns are non-null for the indicator under test.

    Indicators:
      - cpi_yoy_pct           CPI YoY % (CPIAUCSL)
      - t10y2y_spread         10Y - 2Y spread, percentage points (T10Y2Y)
      - unrate_change_3mo     Unemployment delta vs 3 months ago, pp (UNRATE)
      - fedfunds_change_1mo   Effective FFR delta MoM, percentage points (FEDFUNDS)
      - hy_oas_pct            ICE BofA HY OAS, percent (BAMLH0A0HYM2)
*/

WITH cpi AS (
    SELECT
        date,
        literal AS cpi_level,
        LAG(literal, 12) OVER (ORDER BY date) AS cpi_12m_ago
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'CPIAUCSL' AND literal IS NOT NULL
),

cpi_yoy AS (
    SELECT
        date,
        ROUND(((cpi_level / NULLIF(cpi_12m_ago, 0)) - 1) * 100, 2) AS cpi_yoy_pct
    FROM cpi
    WHERE cpi_12m_ago IS NOT NULL
),

t10y2y AS (
    SELECT
        date,
        ROUND(literal, 3) AS t10y2y_spread
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'T10Y2Y' AND literal IS NOT NULL
),

unrate AS (
    SELECT
        date,
        literal AS unrate_level,
        LAG(literal, 3) OVER (ORDER BY date) AS unrate_3mo_ago
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'UNRATE' AND literal IS NOT NULL
),

unrate_delta AS (
    SELECT
        date,
        ROUND(unrate_level - unrate_3mo_ago, 2) AS unrate_change_3mo
    FROM unrate
    WHERE unrate_3mo_ago IS NOT NULL
),

fedfunds AS (
    SELECT
        date,
        literal AS fedfunds_level,
        LAG(literal, 1) OVER (ORDER BY date) AS fedfunds_1mo_ago
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'FEDFUNDS' AND literal IS NOT NULL
),

fedfunds_delta AS (
    SELECT
        date,
        ROUND(ABS(fedfunds_level - fedfunds_1mo_ago), 3) AS fedfunds_change_1mo
    FROM fedfunds
    WHERE fedfunds_1mo_ago IS NOT NULL
),

hy_oas AS (
    SELECT
        date,
        ROUND(literal, 3) AS hy_oas_pct
    FROM {{ ref('stg_fred_series') }}
    WHERE series_code = 'BAMLH0A0HYM2' AND literal IS NOT NULL
),

all_dates AS (
    SELECT date FROM cpi_yoy
    UNION
    SELECT date FROM t10y2y
    UNION
    SELECT date FROM unrate_delta
    UNION
    SELECT date FROM fedfunds_delta
    UNION
    SELECT date FROM hy_oas
)

SELECT
    d.date,
    c.cpi_yoy_pct,
    t.t10y2y_spread,
    u.unrate_change_3mo,
    f.fedfunds_change_1mo,
    h.hy_oas_pct
FROM all_dates AS d
LEFT JOIN cpi_yoy AS c ON d.date = c.date
LEFT JOIN t10y2y AS t ON d.date = t.date
LEFT JOIN unrate_delta AS u ON d.date = u.date
LEFT JOIN fedfunds_delta AS f ON d.date = f.date
LEFT JOIN hy_oas AS h ON d.date = h.date
WHERE d.date >= CURRENT_DATE - INTERVAL 2 YEAR
ORDER BY d.date DESC
