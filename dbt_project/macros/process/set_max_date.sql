--Set max date 
{% macro set_max_date(date_col=None) %}
    {# 
        Returns a COALESCE expression that replaces NULL or open-ended values
        with a standard max date ('9999-12-31').
        #}

    {% if date_col %}
        COALESCE({{ date_col }}, TO_DATE('9999-12-31'))
    {% else %}
        TO_DATE('9999-12-31')
    {% endif %}
{% endmacro %}