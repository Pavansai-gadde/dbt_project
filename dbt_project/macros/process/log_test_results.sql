{% macro log_test_results(results) %}
    {% for r in results  %}
    {% if r.node.resource_type == 'test' %}
        {% set deps = r.node.depends_on.nodes %}
        {% set source_table = deps[0].split('.')[-1] if deps | length > 0 else null %}
        {% set target_table = deps[1].split('.')[-1] if deps | length > 1 else null %}
        {% set test_name = r.node.test_metadata.name %}
        {% set status = r.status %}
        {% set failures = r.failures %}
        {% set environment = target.name %}
        {% set ar = r.adapter_response if r.adapter_response is mapping else {} %}
        {% set sample_rows = ar.get("sample_rows", []) %}


        {% set insert_query %}
       insert into dbt_project_dev.config.data_quality_log (
            source_table,
            target_table,
            test_name,
            test_status,
            failed_records,
            sample_failures,
            run_id,
            job_id,
            environment
        )
        select 
            '{{ source_table }}',
            '{{ target_table }}',
            '{{ test_name }}',
            '{{ status }}',
            {{ failures }},
           '{{ tojson(sample_rows) }}'::variant,
            '{{ invocation_id }}',
            '100',
            '{{ target.name }}'
        
        {% endset %}

    {% do run_query(insert_query) %}

    {% do log("Insert query is executed" , info=True) %}

    {% endif %}

    {% do log("Audit log inserted for source_table: " ~ model_name , info=True) %}
    {% endfor %}
{% endmacro %}