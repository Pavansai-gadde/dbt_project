--check sat link integrity 
{% macro check_link_sat_integrity(link_table, sat_table, link_key, sat_key) %}
    select s.{{ sat_key }}
    from {{ ref(sat_table) }} s
    left join {{ ref(link_table) }} l
      on s.{{ sat_key }} = l.{{ link_key }}
    where l.{{ link_key }} is null
{% endmacro %}
