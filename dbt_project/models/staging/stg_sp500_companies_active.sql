{{
  config(
    description='Active S&P 500 constituents (currently in the index). Filters sp500_companies_raw to rows where date_ended IS NULL.'
  )
}}

SELECT
    symbol,
    company_name,
    sector,
    sub_industry,
    headquarters,
    date_added,
    cik,
    founded,
    source,
    fetched_at,
    date_started
FROM {{ source('staging', 'sp500_companies_raw') }}
WHERE date_ended IS NULL
