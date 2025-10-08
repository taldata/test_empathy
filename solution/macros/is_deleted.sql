{% macro is_deleted(column_name) -%}
    coalesce(try_to_boolean({{ column_name }}), false)
{%- endmacro %}
