{% macro recon_validation(audit_table='config.recon_audit_log') %} 

{% set rows = run_query("select source_db,source_schema,source_table,
target_db,target_schema,target_table,join_keys,reconciliation_type
from dbt_project_dev.config.recon_config
where is_active=TRUE ").rows %}


{% for row in rows %}
    {% set source_db = row[0] %}
    {% set source_schema = row[1] %}
    {% set source_table = row[2] %}
    {% set target_db = row[3] %}
    {% set target_schema = row[4] %}
    {% set target_table = row[5] %}
    {% set join_key = row[6] |replace('\n', '')| replace('[','') | replace(']','') | replace('"','') %}
    {% set reconciliation_type = row[7] %}
    {% set src_value = "null" %}
    {% set trg_value = "null" %}
    {% set missing_keys = [] %}

    
    {% if reconciliation_type== "row_count" %}
        {% set values = compare_counts_between_layers(source_table,target_table) %}

        {% set src_value = values["src_count"]%} 
        {% set trg_value = values["trg_count"]%} 

        {% if values["diff"]==0 %}
            {% set status = 'Pass' %} 
        {% else %}
            {% set status = 'Fail' %} 
        {% endif %}

    {% elif reconciliation_type == "aggregation_validation" %}

        {% set values = aggregation_check(source_table,target_table,join_key) %}

        {% set src_value = values["src_total"]%} 
        {% set trg_value = values["trg_total"]%} 

        {% if values["diff"]==0 %}
            {% set status = 'Pass' %} 
        {% else %}
            {% set status = 'Fail' %} 
        {% endif %}

   {% elif reconciliation_type == "recon_key_match" %}

        {% set values = recon_key_match(source_table,target_table,join_key) %}

        {% set missing_keys = fromjson(values["missing_keys"]) %} 
        {% set status = values["status"]%} 


    {% else %}
        {# do nothing #}
    {% endif %}

{# insert audit record #}
{% set insert_sql %}
    insert into {{ audit_table }} (
        RECON_TYPE,
        SOURCE_DB,
        SOURCE_SCHEEMA,
        SOURCE_TABLE,
        TARGET_DB,
        TARGET_SCHEEMA,
        TARGET_TABLE,
        SOURCE_VALUE,
        TARGET_VALUE,
        MISSING_KEYS,
        STATUS,
        RUN_TS        
    )
    select         
        '{{ reconciliation_type }}',
        '{{ source_db }}',
        '{{ source_schema }}',
        '{{ source_table }}',
        '{{ target_db }}',
        '{{ target_schema }}',
        '{{ target_table }}',
         {{ src_value }},
         {{ trg_value }},
        {{ missing_keys }},
        '{{ status }}',
         current_timestamp
    ;
{% endset %}
{% do run_query(insert_sql) %}

{% do log("Audit log inserted for source_table: " ~ source_table ~ " and target_table: " ~ target_table , info=True) %}


{% endfor %}

{% endmacro %}