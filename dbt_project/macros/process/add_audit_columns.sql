-- add audit columns , if needed we can use it .
{% macro add_audit_columns(in_source_table) %}
    , current_timestamp() as load_dts
    , '{{ in_source_table }}' as record_source
{% endmacro %}