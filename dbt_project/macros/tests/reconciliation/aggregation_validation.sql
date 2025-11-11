{% test aggregation_validation(model,compare_model,column_name) %}

with src as 
(select sum({{ column_name }}) as total from {{ model }}),
trg as 
(select sum({{ column_name }}) as total from {{ compare_model }})
select * from src ,trg
where src.total!=trg.total

{% endtest %}