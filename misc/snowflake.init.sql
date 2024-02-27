use role sysadmin;
use warehouse wh_compute;
create or replace database dbt_data_quality_demo with comment = 'Database for dbt_data_quality_demo';

use role accountadmin;
create or replace resource monitor rm_dbt_data_quality_demo with
  credit_quota = 1
  frequency = daily
  start_timestamp = immediately
  notify_users = ("<YOUR_USER>")
  triggers
    on 100 percent do suspend_immediate
;

create or replace warehouse wh_dbt_data_quality_demo with
  warehouse_type = 'standard'
  warehouse_size = 'xsmall'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true
  resource_monitor = rm_dbt_data_quality_demo
  comment = 'Warehouse for dbt_data_quality_demo';

use role securityadmin;
create or replace role role_dbt_data_quality_demo with comment = 'Role for dbt_data_quality_demo';
create or replace user user_dbt_data_quality_demo with password='<YOUR PASSWORD>' comment = 'User for dbt_data_quality_demo';

grant usage on warehouse wh_dbt_data_quality_demo to role role_dbt_data_quality_demo;
grant usage on database dbt_data_quality_demo to role role_dbt_data_quality_demo;
grant all privileges on database dbt_data_quality_demo to role role_dbt_data_quality_demo;
grant all privileges on all schemas in database dbt_data_quality_demo to role role_dbt_data_quality_demo;
grant all privileges on future schemas in database dbt_data_quality_demo to role role_dbt_data_quality_demo;
grant all privileges on all tables in database dbt_data_quality_demo to role role_dbt_data_quality_demo;
grant all privileges on future tables in database dbt_data_quality_demo to role role_dbt_data_quality_demo;
grant all privileges on all views in database dbt_data_quality_demo to role role_dbt_data_quality_demo;
grant all privileges on future views in database dbt_data_quality_demo to role role_dbt_data_quality_demo;
grant usage, create schema on database dbt_data_quality_demo to role role_dbt_data_quality_demo;
grant role role_dbt_data_quality_demo to role sysadmin;
grant role role_dbt_data_quality_demo to user user_dbt_data_quality_demo;

use role role_dbt_data_quality_demo;
use database dbt_data_quality_demo;
