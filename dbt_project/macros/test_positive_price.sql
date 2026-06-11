{% test positive_price(model, column_name) %}
-- Fails if any row has a price <= 0.
select *
from {{ model }}
where
    {{ column_name }} is not null
    and cast({{ column_name }} as float64) <= 0
{% endtest %}
