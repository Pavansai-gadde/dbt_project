{% macro master_dq_macro() %}

    {% set tables_list = [('raw_customers','raw'),('raw_orders','raw')] %}

    {# {% set raw = now() ~ range(1000000000) | random %}
    {% set id = raw | sha1 %}

    {{ log("Invocation_id: " ~ id, info=True) }}    #}

    {% for table,layer in tables_list %}
        {{ run_dq_checks(table,layer)}} 
    {% endfor %}

{% endmacro %}