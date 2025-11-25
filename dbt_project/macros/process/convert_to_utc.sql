
--convert time zone
{% macro convert_to_utc(timestamp_col, source_tz) %}
{#
    Converts a timestamp column from the given source time zone to UTC.

    Parameters:
      - timestamp_col: column name or expression
      - source_tz: source time zone (Ex: 'America/New_York')
#}

{% set adapter = target.type | lower %}

{% if adapter == 'snowflake' %}
    CONVERT_TIMEZONE('{{ source_tz }}', 'UTC', {{ timestamp_col }})

{% else %}
    -- Default: return as-is with comment
    {{ timestamp_col }}  

{% endif %}

{% endmacro %}