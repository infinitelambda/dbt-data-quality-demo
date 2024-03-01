{% macro sis_deploy__app__test_coverage(app_path='macros/sis') -%}

  {% set ns = generate_database_name() ~ "." ~ generate_schema_name(var("dbt_dq_tool_schema", target.schema)) %}
  {% set query %}

    create schema if not exists {{ ns }};
    create or replace stage {{ ns }}.stage_sis
      directory = ( enable = true )
      comment = 'Named stage for app__test_coverage SiS appilication';

    PUT file://{{ app_path }}/app__test_coverage.py @{{ ns }}.stage_sis
      overwrite=true
      auto_compress=false;

    create or replace streamlit {{ ns }}.app__test_coverage
      root_location = '@{{ ns }}.stage_sis'
      main_file = '/app__test_coverage.py'
      query_warehouse = {{ target.warehouse or 'compute_wh' }}
      comment = 'Streamlit app for the Data Testing Coverage';
  {% endset %}

  {{ log("[RUN]: sis_deploy__app__test_coverage", info=True) if execute }}
  {{ log("query: " ~ query, info=True) if execute }}
  {% set results = run_query(query) %}
  {{ log(results.rows, info=True) }}

{%- endmacro %}
