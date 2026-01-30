{% macro rts_post_hook(src_pk,src_ldts) %}

{# Close old records if new records for same PK exist #}
update {{ this }}
set is_active = FALSE,
    end_dts = current_timestamp()
where is_active = TRUE
and exists (
    select 1
    from {{ this }} as src
    where src.{{ src_pk }} = {{ this }}.{{ src_pk }}
    and src.{{ src_ldts }} > {{ this }}.{{ src_ldts }}
)

{% endmacro %}