
---Audit log table create and will use this macro in post hook.

{% macro model_audit_log(
    audit_table='config.MODEL_AUDIT_LOG_TABLE',
    source_table='raw_orders',
    comments=''
) %}
{# 
    Inserts a detailed audit record for each model run, designed for dbt post-hooks.

    Parameters:
      - audit_table: fully qualified table for logging
      - source_table: optional ref() or string — used for source row count
      - comments: free text, e.g., 'Hub Load' or 'Reconciliation Model'
#}

{% set model_name = model.name %} 
{% set schema_name = model.schema %}
{% set database_name = target.database %}
{% set run_id = invocation_id %}
{% set environment = target.name %}
{% set project = project_name %}
{# {% set status = run_result.status if execute else 'compiled' %} #}
{% set status = 'pass' %}
{% set started_at = run_started_at %}
{% set completed_at = current_timestamp() %}
{% set user = target.user if target.user is not none else 'dbt_user' %}

{# calculate execution duration in seconds #}
{% set duration_expr = "DATEDIFF(second, '" ~ started_at ~ "', current_timestamp())" %}
{% set duration_result = run_query("select " ~ duration_expr ~ " as duration") %}
{% set duration_seconds = duration_result.columns[0].values()[0] if execute else 0 %}

{# get target row count (from adapter response if available) #}
{% set target_row_count = (
    run_result.adapter_response['rows_affected']
    if execute and 'rows_affected' in run_result.adapter_response
    else 0
) %}

{# get source row count if a table is provided #}
{% if source_table is not none %}
    {% set src_sql %}
        select count(*) as cnt from {{ source_table }}
    {% endset %}
    {% set src_result = run_query(src_sql) %}
    {% set source_row_count = src_result.columns[0].values()[0] if execute else 0 %}
{% else %}
    {% set source_row_count = 0 %}
{% endif %}

{# ensure audit table exists #}
{% set create_table_sql %}
    create table if not exists {{ audit_table }} (
        run_id VARCHAR,
        project_name VARCHAR,
        environment VARCHAR,
        database_name VARCHAR,
        schema_name VARCHAR,
        model_name VARCHAR,
        user_name VARCHAR,
        started_at timestamp,
        completed_at timestamp,
        duration_seconds number,
        status VARCHAR,
        source_row_count number,
        target_row_count number,
        comments VARCHAR
    );
{% endset %}
{% do run_query(create_table_sql) %}

{# insert audit record #}
{% set insert_sql %}
    insert into {{ audit_table }} (
        run_id,
        project_name,
        environment,
        database_name,
        schema_name,
        model_name,
        user_name,
        started_at,
        completed_at,
        duration_seconds,
        status,
        source_row_count,
        target_row_count,
        comments
    )
    values (
        '{{ run_id }}',
        '{{ project }}',
        '{{ environment }}',
        '{{ database_name }}',
        '{{ schema_name }}',
        '{{ model_name }}',
        '{{ user }}',
        '{{ started_at }}',
        '{{ completed_at }}',
        {{ duration_seconds }},
        '{{ status }}',
        {{ source_row_count }},
        {{ target_row_count }},
        '{{ comments }}'
    );
{% endset %}
{% do run_query(insert_sql) %}

{{ log("Audit log entry inserted for model: " ~ model_name ~ " | Duration: " ~ duration_seconds ~ "s", info=True) }}
{% endmacro %}
