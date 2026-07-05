{% macro ohlc_source_tables() %}
    {% set tables = [
        'us_sector_etfs_raw',
        'currency_etfs_raw',
        'commodity_etfs_raw',
        'major_indices_raw',
        'fixed_income_etfs_raw',
        'global_markets_raw',
        'sp500_companies_prices_raw'
    ] %}
    {{ return(tables) }}
{% endmacro %}

{% macro split_eligible_tables() %}
    {% set tables = [
        {'raw': 'sp500_companies_prices_raw', 'stg': 'stg_sp500_companies_prices'}
    ] %}
    {{ return(tables) }}
{% endmacro %}

{% macro commodity_source_tables() %}
    {% set tables = [
        'energy_commodities_raw',
        'input_commodities_raw',
        'agriculture_commodities_raw'
    ] %}
    {{ return(tables) }}
{% endmacro %}
