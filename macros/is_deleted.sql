{% macro is_deleted(column_name) -%}
    {{ exceptions.raise_compiler_error('Implement the is_deleted macro to standardize soft delete filtering.') }}
{%- endmacro %}
