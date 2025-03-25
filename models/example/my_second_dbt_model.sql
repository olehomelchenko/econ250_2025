
-- Use the `ref` function to select from other models

select *,
"test value" as new_column
from {{ ref('my_first_dbt_model') }}
where id = 1
