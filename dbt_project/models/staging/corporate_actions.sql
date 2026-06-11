{{ config(materialized='table') }}

-- Detects stock splits and dividends from multiple sources:
-- 1. splits_api: authoritative split data from MarketStack /splits endpoint (highest priority)
-- 2. api_reported: rows where split_factor != 1 in OHLC data
-- 3. heuristic: overnight price ratio near clean integer reciprocals (catches unreported splits)
-- 4. Dividends: rows where dividend > 0

{% set split_eligible = split_eligible_tables() %}

-- Authoritative splits from the MarketStack /splits API.
-- Fan out across all split-eligible source tables so symbols that appear in
-- both S&P 500 and NASDAQ get authoritative split data in both universes.
WITH splits_api AS (
{% for entry in split_eligible %}
    SELECT
        '{{ entry.raw }}' AS source_table,
        s.symbol,
        CAST(s.date AS DATE) AS date,
        'split' AS action_type,
        s.split_factor,
        0.0 AS dividend_amount,
        'splits_api' AS detection_method
    FROM {{ source('staging', 'sp500_splits_raw') }} AS s
    INNER JOIN (
        SELECT DISTINCT symbol
        FROM {{ ref(entry.stg) }}
    ) AS t ON s.symbol = t.symbol
    WHERE s.split_factor IS NOT NULL
      AND s.split_factor != 1
    {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
),

-- Detect when MarketStack raw prices already reflect the split BEFORE the
-- official API split date.  If the open/prev_close ratio on the trading day
-- before the split date is within 10% of 1/split_factor, the split was
-- already effective that day → shift the date back.
-- Only checked for split_factor >= 1.2 to avoid false positives from tiny
-- factors (e.g. 1.01) where normal daily volatility could match.
splits_api_adjusted AS (
    SELECT
        source_table,
        symbol,
        CASE
            WHEN split_factor >= 1.2
                 AND prev_close IS NOT NULL
                 AND prev_close > 0
                 AND open > 0
                 AND ABS(
                     open / prev_close - 1.0 / split_factor
                 ) / (1.0 / split_factor) < 0.10
            THEN prior_price_date  -- shift to the day-before date
            ELSE date
        END AS date,
        action_type,
        split_factor,
        dividend_amount,
        detection_method
    FROM (
        SELECT
            sa.*,
            p.date AS prior_price_date,
            p.open,
            p.prev_close,
            ROW_NUMBER() OVER (
                PARTITION BY sa.source_table, sa.symbol, sa.date, sa.action_type
                ORDER BY p.date DESC
            ) AS prior_price_rank
        FROM splits_api AS sa
        LEFT JOIN (
            SELECT
                symbol,
                CAST(date AS DATE) AS date,
                open,
                LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
            FROM {{ ref('stg_sp500_companies_prices') }}
        ) AS p
            ON sa.symbol = p.symbol
            AND p.date < sa.date
    )
    WHERE prior_price_rank = 1
),

-- Collect all OHLC-based split and dividend detections
ohlc_based AS (
{% for entry in split_eligible %}

    -- API-reported splits for {{ entry.raw }}
    SELECT
        '{{ entry.raw }}' AS source_table,
        src.symbol,
        CAST(src.date AS DATE) AS date,
        'split' AS action_type,
        src.split_factor,
        0.0 AS dividend_amount,
        'api_reported' AS detection_method
    FROM {{ ref(entry.stg) }} AS src
    WHERE src.split_factor IS NOT NULL
      AND src.split_factor != 1

    UNION ALL

    -- Heuristic split detection for {{ entry.raw }}
    SELECT
        '{{ entry.raw }}' AS source_table,
        h.symbol,
        h.date,
        'split' AS action_type,
        CASE
            WHEN ABS(h.open / NULLIF(h.prev_close, 0) - 1.0 / 10) / (1.0 / 10) < 0.05 THEN 10
            WHEN ABS(h.open / NULLIF(h.prev_close, 0) - 1.0 / 5) / (1.0 / 5) < 0.05 THEN 5
            WHEN ABS(h.open / NULLIF(h.prev_close, 0) - 1.0 / 4) / (1.0 / 4) < 0.05 THEN 4
            WHEN ABS(h.open / NULLIF(h.prev_close, 0) - 1.0 / 3) / (1.0 / 3) < 0.05 THEN 3
            WHEN ABS(h.open / NULLIF(h.prev_close, 0) - 1.0 / 2) / (1.0 / 2) < 0.05 THEN 2
            WHEN ABS(h.open / NULLIF(h.prev_close, 0) - 2.0) / 2.0 < 0.05 THEN 0.5
            WHEN ABS(h.open / NULLIF(h.prev_close, 0) - 3.0) / 3.0 < 0.05 THEN 1.0 / 3
            WHEN ABS(h.open / NULLIF(h.prev_close, 0) - 4.0) / 4.0 < 0.05 THEN 0.25
        END AS split_factor,
        0.0 AS dividend_amount,
        'heuristic' AS detection_method
    FROM (
        SELECT
            symbol,
            CAST(date AS DATE) AS date,
            open,
            split_factor,
            LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
        FROM {{ ref(entry.stg) }}
    ) AS h
    WHERE h.prev_close IS NOT NULL
      AND h.prev_close > 0
      AND h.open > 0
      AND (h.split_factor IS NULL OR h.split_factor = 1)
      AND (
          ABS(h.open / h.prev_close - 1.0 / 10) / (1.0 / 10) < 0.05
          OR ABS(h.open / h.prev_close - 1.0 / 5) / (1.0 / 5) < 0.05
          OR ABS(h.open / h.prev_close - 1.0 / 4) / (1.0 / 4) < 0.05
          OR ABS(h.open / h.prev_close - 1.0 / 3) / (1.0 / 3) < 0.05
          OR ABS(h.open / h.prev_close - 1.0 / 2) / (1.0 / 2) < 0.05
          OR ABS(h.open / h.prev_close - 2.0) / 2.0 < 0.05
          OR ABS(h.open / h.prev_close - 3.0) / 3.0 < 0.05
          OR ABS(h.open / h.prev_close - 4.0) / 4.0 < 0.05
      )

    UNION ALL

    -- Dividends for {{ entry.raw }}
    SELECT
        '{{ entry.raw }}' AS source_table,
        src.symbol,
        CAST(src.date AS DATE) AS date,
        'dividend' AS action_type,
        1.0 AS split_factor,
        src.dividend AS dividend_amount,
        'api_reported' AS detection_method
    FROM {{ ref(entry.stg) }} AS src
    WHERE src.dividend IS NOT NULL
      AND src.dividend > 0

    {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
)

-- Final output: splits_api rows always win.
-- Both api_reported and heuristic splits are excluded within ±5 days of an
-- API split to prevent duplicates when splits_api_adjusted shifts the
-- authoritative date (e.g., MarketStack raw prices reflect the split 1 day
-- before the official API split date).
-- Dividends always pass through (they don't conflict with split rows).
SELECT * FROM splits_api_adjusted

UNION ALL

-- api_reported and dividend rows: ±5 day window dedup against splits_api_adjusted.
-- Uses a window (not exact date) because splits_api_adjusted may have shifted
-- the authoritative date by 1+ days when raw prices were already post-split.
SELECT o.*
FROM ohlc_based AS o
WHERE o.detection_method != 'heuristic'
  AND NOT EXISTS (
      SELECT 1
      FROM splits_api_adjusted AS s
      WHERE o.source_table = s.source_table
        AND o.symbol = s.symbol
        AND o.action_type = 'split'
        AND s.action_type = 'split'
        AND ABS(DATE_DIFF(o.date, s.date, DAY)) <= 5
  )

UNION ALL

-- heuristic rows: wider ±5 day window dedup against splits_api_adjusted
SELECT o.*
FROM ohlc_based AS o
WHERE o.detection_method = 'heuristic'
  AND NOT EXISTS (
      SELECT 1
      FROM splits_api_adjusted AS s
      WHERE o.source_table = s.source_table
        AND o.symbol = s.symbol
        AND o.action_type = 'split'
        AND s.action_type = 'split'
        AND ABS(DATE_DIFF(o.date, s.date, DAY)) <= 5
  )
