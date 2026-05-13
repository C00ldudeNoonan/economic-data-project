{% test usd_currency_only(model, column_name) %}
-- Fails if any row has a non-USD price_currency value.
-- NULL is allowed (older data before the field was added).
select
    symbol,
    cast(date as date) as date,
    {{ column_name }}
from {{ model }}
where
    {{ column_name }} is not null
    and lower({{ column_name }}) != 'usd'
{% endtest %}
