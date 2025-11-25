{{ config(
    post_hook = "call dbt_project_dev.test_framework.load_dbt_model_errors(); "
)}}

select '1'