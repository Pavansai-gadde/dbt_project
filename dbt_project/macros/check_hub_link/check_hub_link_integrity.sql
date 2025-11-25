--check hub link integrity 
{% macro check_hub_link_integrity(hub_table, link_table, hub_key, link_key) %}
    select l.{{ link_key }}
    from {{ ref(link_table) }} l
    left join {{ ref(hub_table) }} h
      on l.{{ link_key }} = h.{{ hub_key }}
    where h.{{ hub_key }} is null
{% endmacro %}