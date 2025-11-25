
--Dynamic seviourity
{% macro dynamic_severity(default='warn', prod_severity='error', threshold=None, failure_ratio=None) %}
    {# 
        Dynamic severity logic for DBT Cloud tests.

        Parameters:
          default        → default severity (usually 'warn')
          prod_severity  → severity in prod environment
          threshold      → optional threshold (numeric), e.g. 0.05 (5%)
          failure_ratio  → optional ratio to dynamically decide severity

        Usage examples:
          {{ dynamic_severity() }}
          {{ dynamic_severity(default='warn', prod_severity='error') }}
          {{ dynamic_severity(threshold=0.05, failure_ratio=0.12) }}
    #}

    {% set env = target.name | lower %}

    {# -- Environment-based severity -- #}
    {% if failure_ratio is none %}
        {% if env in ['prod', 'production'] %}
            {{ prod_severity }}
        {% else %}
            {{ default }}
        {% endif %}

    {# -- Data-quality ratio-based severity -- #}
    {% else %}
        {% if failure_ratio > threshold %}
            {{ prod_severity }}
        {% else %}
            {{ default }}
        {% endif %}
    {% endif %}
{% endmacro %}