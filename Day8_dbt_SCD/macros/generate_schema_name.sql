{% macro generate_schema_name(custom_schema_name, node) %}
    {{ custom_schema_name if custom_schema_name is not none else node.schema }}
{% endmacro %}