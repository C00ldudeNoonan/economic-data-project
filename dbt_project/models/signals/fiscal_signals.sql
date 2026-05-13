-- Fiscal / Government signals
-- Uses: GFDEGDQ188S (Debt-to-GDP), FYFSGDA188S (Deficit-to-GDP), A091RC1Q027SBEA (Interest Payments)

with debt_gdp as (
    select
        date,
        value as debt_gdp_pct,
        lag(value, 4) over (order by date) as debt_gdp_1y_ago,
        lag(value, 8) over (order by date) as debt_gdp_2y_ago
    from {{ ref('stg_fred_series') }}
    where series_code = 'GFDEGDQ188S'
      and value is not null
),

interest_payments as (
    select
        date,
        value as interest_payment,
        lag(value, 4) over (order by date) as interest_1y_ago
    from {{ ref('stg_fred_series') }}
    where series_code = 'A091RC1Q027SBEA'
      and value is not null
),

deficit_gdp as (
    select
        date,
        value as deficit_gdp_pct,
        lag(value, 1) over (order by date) as deficit_1y_ago,
        row_number() over (order by date desc) as rn
    from {{ ref('stg_fred_series') }}
    where series_code = 'FYFSGDA188S'
      and value is not null
),

latest_deficit as (
    select * from deficit_gdp where rn = 1
),

combined as (
    select
        d.date,
        d.debt_gdp_pct,
        d.debt_gdp_1y_ago,
        d.debt_gdp_2y_ago,
        d.debt_gdp_pct - coalesce(d.debt_gdp_1y_ago, d.debt_gdp_pct) as debt_gdp_1y_change,
        d.debt_gdp_pct - coalesce(d.debt_gdp_2y_ago, d.debt_gdp_pct) as debt_gdp_2y_change,
        i.interest_payment,
        i.interest_1y_ago,
        case
            when i.interest_1y_ago is not null and i.interest_1y_ago > 0
            then ((i.interest_payment - i.interest_1y_ago) / i.interest_1y_ago) * 100
            else null
        end as interest_yoy_growth,
        ld.deficit_gdp_pct,
        ld.deficit_gdp_pct - coalesce(ld.deficit_1y_ago, ld.deficit_gdp_pct) as deficit_yoy_change
    from debt_gdp d
    left join interest_payments i on d.date = i.date
    cross join latest_deficit ld
)

select
    date,
    debt_gdp_pct,
    debt_gdp_1y_change,
    debt_gdp_2y_change,
    interest_payment,
    interest_yoy_growth,
    deficit_gdp_pct,
    deficit_yoy_change,

    -- Debt level status
    case
        when debt_gdp_pct >= 130 then 'high'
        when debt_gdp_pct >= 100 then 'medium'
        when debt_gdp_pct >= 60 then 'normal'
        else 'low'
    end as debt_level_status,

    -- Debt trajectory (rising fast?)
    case
        when debt_gdp_1y_change > 5 then 'high'
        when debt_gdp_1y_change > 2 then 'medium'
        when debt_gdp_1y_change > 0 then 'low'
        else 'normal'
    end as debt_trajectory_status,

    -- Interest burden growing?
    case
        when interest_yoy_growth > 20 then 'high'
        when interest_yoy_growth > 10 then 'medium'
        when interest_yoy_growth > 0 then 'low'
        else 'normal'
    end as interest_burden_status,

    -- Deficit level
    case
        when deficit_gdp_pct < -6 then 'high'
        when deficit_gdp_pct < -3 then 'medium'
        when deficit_gdp_pct < 0 then 'low'
        else 'normal'
    end as deficit_status

from combined
order by date desc
