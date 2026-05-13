{% test value_in_range(model, column_name, min_value=none, max_value=none) %}

{# Custom test: validates that a column's values fall within an expected range #}
{# Null values are excluded (use not_null test separately if needed) #}

SELECT
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
{% if min_value is not none %}
    AND {{ column_name }} < {{ min_value }}
{% endif %}
{% if max_value is not none %}
    {% if min_value is not none %}
    OR ({{ column_name }} IS NOT NULL AND {{ column_name }} > {{ max_value }})
    {% else %}
    AND {{ column_name }} > {{ max_value }}
    {% endif %}
{% endif %}

{% endtest %}
