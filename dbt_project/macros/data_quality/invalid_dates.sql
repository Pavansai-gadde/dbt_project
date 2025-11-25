
{% macro invalid_dates(model, effective_col='row_effective_date', end_col='row_end_date') %}
    {#
        Detects rows in an SCD2 table where the row_end_date is earlier than row_effective_date.
        Returns rows → test FAIL.
        Returns zero rows → test PASS.

        Usage in YAML:
          tests:
            - scd2_invalid_dates:
                effective_col: row_effective_date
                end_col: row_end_date
    #}

    select
        *,
        {{ effective_col }} as effective_dt,
        {{ end_col }} as end_dt
    from {{ model }}
    where {{ end_col }} < {{ effective_col }}
{% endmacro %}