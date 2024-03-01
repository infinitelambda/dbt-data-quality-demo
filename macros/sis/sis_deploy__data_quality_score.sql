{% macro sis_deploy__app__data_quality_score(app_path='macros/sis') -%}

  {% set ns = generate_database_name() ~ "." ~ generate_schema_name(var("dbt_dq_tool_schema", target.schema)) %}
  {% set query %}

    create schema if not exists {{ ns }};
    create or replace stage {{ ns }}.stage_sis
      directory = ( enable = true )
      comment = 'Named stage for app__data_quality_score SiS appilication';

    PUT file://{{ app_path }}/app__data_quality_score.py @{{ ns }}.stage_sis
      overwrite=true
      auto_compress=false;

    create or replace streamlit {{ ns }}.app__data_quality_score
      root_location = '@{{ ns }}.stage_sis'
      main_file = '/app__data_quality_score.py'
      query_warehouse = {{ target.warehouse or 'compute_wh' }}
      comment = 'Streamlit app for the Data Quality Score';
  {% endset %}

  {{ log("[RUN]: sis_deploy__app__data_quality_score", info=True) if execute }}
  {{ log("query: " ~ query, info=True) if execute }}
  {% set results = run_query(query) %}
  {{ log(results.rows, info=True) }}

{%- endmacro %}
