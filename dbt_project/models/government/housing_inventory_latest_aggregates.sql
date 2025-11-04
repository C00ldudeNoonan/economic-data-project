{{ config(
    materialized='table'
) }}


With inventory As (
    Select
        data_type_code As series_code,
        seasonally_adj,
        category_code,
        cast(cell_value As float) As clean_value,
        error_data,
        time,
        series_name,
        plot_grouping,
        cast((Case
            When
                right(time, 2) = 'Q1'
                Then cast((left(time, 4) || '-01-01') As date)
            When
                right(time, 2) = 'Q2'
                Then cast((left(time, 4) || '-04-01') As date)
            When
                right(time, 2) = 'Q3'
                Then cast((left(time, 4) || '-07-01') As date)
            When
                right(time, 2) = 'Q4'
                Then cast((left(time, 4) || '-10-01') As date)
        End) As date) As month,
        'Quarterly' As date_grain
    From {{ ref('stg_housing_inventory') }}
    Where cell_value <> '(z)' And error_data = 'no'
),

date_ranges As (
    Select
        series_code,
        series_name,
        date_grain,
        month,
        clean_value,
        lag(clean_value, 1) Over (
            Partition By series_code
            Order By month
        ) As value_3m_ago,
        lag(clean_value, 2) Over (
            Partition By series_code
            Order By month
        ) As value_6m_ago,
        lag(clean_value, 4) Over (
            Partition By series_code
            Order By month
        ) As value_1y_ago
    From inventory
),

calc_view As (
    Select
        series_code,
        series_name,
        date_grain,
        month,
        clean_value As current_value,
        value_3m_ago,
        value_6m_ago,
        value_1y_ago,
        Case
            When value_3m_ago Is NULL Or value_3m_ago = 0 Then NULL
            Else round((clean_value - value_3m_ago) / (value_3m_ago), 2)
        End As pct_change_3m,
        Case
            When value_6m_ago Is NULL Or value_6m_ago = 0 Then NULL
            Else round((clean_value - value_3m_ago) / (value_6m_ago), 2)
        End As pct_change_6m,
        Case
            When value_1y_ago Is NULL Or value_1y_ago = 0 Then NULL
            Else round((clean_value - value_3m_ago) / (value_1y_ago), 2)
        End As pct_change_1y
    From date_ranges
),

max_date_view As (
    Select
        series_code,
        max(month) As month

    From calc_view
    Group By
        series_code,
),

final As (
    Select
        calc_view.series_code,
        calc_view.series_name,
        calc_view.month,
        calc_view.current_value,
        calc_view.pct_change_3m,
        calc_view.pct_change_6m,
        calc_view.pct_change_1y,
        calc_view.date_grain
    From calc_view
    Inner Join max_date_view
        On
            calc_view.series_code = max_date_view.series_code
            And calc_view.month = max_date_view.month
)

Select * From final
