
--For multiple DBT environments (dev, qa, prod), 
-- Need to chaneg the env names once as per env desgin.

{% macro dynamic_severity_env_map(env_map={'dev': 'warn', 'qa': 'warn','uat': 'warn', 'prod': 'error'}) %}
    {% set env = target.name | lower %}
    {% if env in env_map %}
        {{ env_map[env] }}
    {% else %}
        warn
    {% endif %}
{% endmacro %}


