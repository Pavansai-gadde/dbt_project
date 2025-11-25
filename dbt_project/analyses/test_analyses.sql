select rule_id,
model_name,
rule_name,
rule_expression,
severity
from {{ source('config','dq_rules') }}
where model_name = 'new'