{{ config(severity='warn') }}

/*
    Surface upstream grain violations before they reach semantic-layer metrics.

    The semantic layer enforces one row per asset/date, but upstream duplicates
    still matter because they can hide ingestion or split-adjustment issues.
*/

WITH duplicate_checks AS (
    SELECT
        'sp500_companies_analysis_return' AS model_name,
        CONCAT(symbol, ':', exchange, ':', CAST(date AS STRING)) AS grain_key,
        COUNT(*) AS duplicate_count
    FROM {{ ref('sp500_companies_analysis_return') }}
    GROUP BY symbol, exchange, date
    HAVING COUNT(*) > 1

    UNION ALL

    SELECT
        'us_sector_analysis_return' AS model_name,
        CONCAT(symbol, ':', exchange, ':', CAST(date AS STRING)) AS grain_key,
        COUNT(*) AS duplicate_count
    FROM {{ ref('us_sector_analysis_return') }}
    GROUP BY symbol, exchange, date
    HAVING COUNT(*) > 1
)

SELECT *
FROM duplicate_checks
