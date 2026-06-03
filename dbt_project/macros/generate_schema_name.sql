{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set env_suffix = '' -%}
    {%- if target.name == 'dev' -%}
        {%- set env_suffix = '_dev' -%}
    {%- elif target.name == 'staging' -%}
        {%- set env_suffix = '_staging' -%}
    {%- endif -%}

    {%- if custom_schema_name is none -%}
        {{ target.dataset }}{{ env_suffix }}
    {%- else -%}
        {{ custom_schema_name | trim }}{{ env_suffix }}
    {%- endif -%}
{%- endmacro %}
