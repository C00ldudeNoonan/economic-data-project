{% test unique_combination(model, combination_of_columns) %}

{# Custom test: validates that the combination of columns is unique across the model #}

SELECT
    {{ combination_of_columns | join(', ') }},
    COUNT(*) AS row_count
FROM {{ model }}
GROUP BY {{ combination_of_columns | join(', ') }}
HAVING COUNT(*) > 1

{% endtest %}
