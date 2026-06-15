-- International Trade & Dollar signals
-- Uses: DTWEXBGS (Broad Dollar Index), BOPGSTB (Trade Balance), DTWEXEMEGS (EM Dollar Index)

with dollar_broad as (
    select
        DATE_TRUNC(date, MONTH) as month,
        avg(value) as dollar_broad_avg
    from {{ ref('stg_fred_series') }}
    where series_code = 'DTWEXBGS'
      and value is not null
    group by month
),

dollar_em as (
    select
        DATE_TRUNC(date, MONTH) as month,
        avg(value) as em_dollar_avg
    from {{ ref('stg_fred_series') }}
    where series_code = 'DTWEXEMEGS'
      and value is not null
    group by month
),

trade_balance as (
    select
        date as month,
        value as trade_balance
    from {{ ref('stg_fred_series') }}
    where series_code = 'BOPGSTB'
      and value is not null
),

combined as (
    select
        db.month as date,
        db.dollar_broad_avg,
        de.em_dollar_avg,
        tb.trade_balance,
        lag(db.dollar_broad_avg, 3) over (order by db.month) as dollar_3m_ago,
        lag(db.dollar_broad_avg, 6) over (order by db.month) as dollar_6m_ago,
        lag(db.dollar_broad_avg, 12) over (order by db.month) as dollar_12m_ago,
        lag(tb.trade_balance, 12) over (order by db.month) as trade_12m_ago
    from dollar_broad db
    left join dollar_em de on db.month = de.month
    left join trade_balance tb on db.month = tb.month
),

with_changes as (
    select
        date,
        dollar_broad_avg,
        em_dollar_avg,
        trade_balance,
        case when dollar_3m_ago > 0
            then ((dollar_broad_avg - dollar_3m_ago) / dollar_3m_ago) * 100
            else null
        end as dollar_3m_pct_change,
        case when dollar_6m_ago > 0
            then ((dollar_broad_avg - dollar_6m_ago) / dollar_6m_ago) * 100
            else null
        end as dollar_6m_pct_change,
        case when dollar_12m_ago > 0
            then ((dollar_broad_avg - dollar_12m_ago) / dollar_12m_ago) * 100
            else null
        end as dollar_12m_pct_change,
        case when trade_12m_ago is not null and trade_12m_ago != 0
            then ((trade_balance - trade_12m_ago) / abs(trade_12m_ago)) * 100
            else null
        end as trade_12m_pct_change,
        -- EM vs Broad divergence (positive = EM weakening faster)
        case
            when dollar_broad_avg > 0 and em_dollar_avg > 0
            then ((em_dollar_avg / lag(em_dollar_avg, 3) over (order by date) - 1)
                - (dollar_broad_avg / dollar_3m_ago - 1)) * 100
            else null
        end as em_broad_divergence
    from combined
)

select
    date,
    dollar_broad_avg,
    em_dollar_avg,
    trade_balance,
    dollar_3m_pct_change,
    dollar_6m_pct_change,
    dollar_12m_pct_change,
    trade_12m_pct_change,
    em_broad_divergence,

    -- Dollar momentum (strong moves in either direction are signals)
    case
        when abs(dollar_3m_pct_change) > 5 then 'high'
        when abs(dollar_3m_pct_change) > 3 then 'medium'
        when abs(dollar_3m_pct_change) > 1 then 'low'
        else 'normal'
    end as dollar_momentum_status,

    -- Trade deficit trend
    case
        when trade_12m_pct_change < -15 then 'high'
        when trade_12m_pct_change < -10 then 'medium'
        when trade_12m_pct_change < 0 then 'low'
        else 'normal'
    end as trade_deficit_status,

    -- EM stress (EM dollar rising faster than broad = EM stress)
    case
        when em_broad_divergence > 3 then 'high'
        when em_broad_divergence > 1.5 then 'medium'
        when em_broad_divergence > 0 then 'low'
        else 'normal'
    end as em_stress_status

from with_changes
order by date desc
