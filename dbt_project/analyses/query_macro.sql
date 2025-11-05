select {{ multiply(4,5) }} as result
from {{ ref('bronze_sales') }} limit 10