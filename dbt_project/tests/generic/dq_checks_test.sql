{% test dq_checks_test(model,model_name,layer_name) %}

-- Run DQ Checks macro 
{% do run_dq_checks(model_name,layer_name) %}

-- Query to return 0 rows
select 0 as result 
where 1=0 
 
{% endtest %}