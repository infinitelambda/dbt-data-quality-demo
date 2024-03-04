select
    *
from {{ ref('my_second_dbt_model' )}}
where true 
and id > 4