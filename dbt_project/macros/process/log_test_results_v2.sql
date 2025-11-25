{% macro log_test_results_v2() %}

{% set rows = run_query("select source_db,source_schema,source_table,
target_db,target_schema,target_table,join_keys,reconciliation_type
from dbt_project_dev.config.recon_config ").rows %}

{% for row in rows %}
    {% set source_db = row[0] %}
    {% set source_schema = row[1] %}
    {% set source_table = row[2] %}
    {% set target_db = row[3] %}
    {% set target_schema = row[4] %}
    {% set target_table = row[5] %}
    {% set join_key = row[6] |replace('\n', '')| replace('[','') | replace(']','') | replace('"','') %}
    {% set reconciliation_type = row[7] %}

    {% do log(join_key, info = True ) %}

{% endfor %}

{% endmacro %}