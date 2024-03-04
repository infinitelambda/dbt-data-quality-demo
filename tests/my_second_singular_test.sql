{{ config(severity = 'warn') }}

with a as (
    select id from {{ ref('my_first_dbt_model' )}}
    union all
    select id from {{ ref('my_second_dbt_model' )}}
)

select id from a
{# where true 
and id != 4 #}
where 1=0