{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {# Isso garante que o dbt use APENAS o nome que você definiu no yml #}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}