{% macro run_error_proc() %}

    {% set query %}
        call dbt_project_dev.test_framework.load_dbt_model_errors();
    {% endset %}

    {% do run_query(query) %}

{% endmacro %}