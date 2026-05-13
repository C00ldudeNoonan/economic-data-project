{{ config(
    tags=['data_quality']
) }}

-- Unified data quality anomaly report.
-- Combines statistical anomaly checks into a single table for triage and review.
-- Deterministic checks (OHLC logic, currency, positive price) are dbt tests
-- on the staging models and emit warnings during dbt build.

select * from {{ ref('dq_stale_prices') }}
union all
select * from {{ ref('dq_return_spikes') }}
union all
select * from {{ ref('dq_zscore_anomalies') }}
union all
select * from {{ ref('dq_commodity_anomalies') }}
