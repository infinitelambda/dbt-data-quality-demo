import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import avg

st.set_page_config(layout="wide")

# Base
session = get_active_session()

# Header
st.title("Data Quality Score")
st.write("""
Source data captured by [dq-tools](https://infinitelambda.github.io/dq-tools/)
""")


# KPIs
st.subheader("KPIs Overview")
st.caption("Today Statistics:")
sql = f"""
    WITH source AS (
        SELECT * FROM dbt_dat.bi_dq_metrics
    ),
    today AS (
        SELECT  dq_dimension,
                DATE_TRUNC('day', run_time) AS run_date,
                AVG((rows_processed - rows_failed) * 1.00 / NULLIF(rows_processed,0)) * 100 AS dq_score,
                AVG(rows_processed)::INT AS rows_processed,
                AVG(rows_failed)::INT AS rows_failed
        FROM    source
        WHERE   DATE_TRUNC('day', run_time) = (SELECT MAX(DATE_TRUNC('day', run_time)) FROM source)
        GROUP BY ALL
    ),
    yesterday AS (
        SELECT  dq_dimension,
                AVG((rows_processed - rows_failed) * 1.00 / NULLIF(rows_processed,0)) * 100 AS dq_score,
                AVG(rows_processed)::INT AS rows_processed,
                AVG(rows_failed)::INT AS rows_failed
        FROM    source
        WHERE   DATE_TRUNC('day', run_time) = (
                    SELECT  MAX(DATE_TRUNC('day', run_time)) 
                    FROM    source 
                    WHERE   run_time < (SELECT MIN(run_date) FROM today)
                )
        GROUP BY ALL
    )
    SELECT      today.dq_dimension AS dq_kpi,
                COALESCE(today.dq_score, 0) AS dq_score,
                COALESCE(today.dq_score - yesterday.dq_score, 0) AS dq_score_delta,
                COALESCE(today.rows_processed, 0) AS rows_processed,
                COALESCE(today.rows_processed - yesterday.rows_processed, 0) AS rows_processed_delta,
                COALESCE(today.rows_failed, 0) AS rows_failed,
                COALESCE(today.rows_failed - yesterday.rows_failed, 0) AS rows_failed_delta
    FROM        today
    LEFT JOIN   yesterday USING (dq_dimension)
    
"""
data = session.sql(sql)
with st.expander(label="Data"):
    st.dataframe(data)

card_columns = st.columns(3)
card_data = data.select(
    avg(data.dq_score).alias("dq_score"),
    avg(data.dq_score_delta).alias("dq_score_delta"),
    avg(data.rows_processed).alias("rows_processed"),
    avg(data.rows_processed_delta).alias("rows_processed_delta"),
    avg(data.rows_failed).alias("rows_failed"),
    avg(data.rows_failed_delta).alias("rows_failed_delta")
).collect()
with card_columns[0]:
    st.metric(label="Quality Score", value=float(card_data[0]["DQ_SCORE"]), delta=float(card_data[0]["DQ_SCORE_DELTA"]))
with card_columns[1]:
    st.metric(label="Rows Processed", value=int(card_data[0]["ROWS_PROCESSED"]), delta=int(card_data[0]["ROWS_PROCESSED_DELTA"]))
with card_columns[2]:
    st.metric(label="Rows Failed", value=int(card_data[0]["ROWS_FAILED"]), delta=int(card_data[0]["ROWS_FAILED_DELTA"]), delta_color="inverse")
    
kpis_columns = st.columns(6)
with kpis_columns[0]:
    data_kpi = data.filter("DQ_KPI = 'Accuracy'").collect()
    st.metric(label="Accuracy", value=float(data_kpi[0]["DQ_SCORE"]), delta=float(data_kpi[0]["DQ_SCORE_DELTA"]))
with kpis_columns[1]:
    data_kpi = data.filter("DQ_KPI = 'Completeness'").collect()
    st.metric(label="Completeness", value=float(data_kpi[0]["DQ_SCORE"]), delta=float(data_kpi[0]["DQ_SCORE_DELTA"]))
with kpis_columns[2]:
    data_kpi = data.filter("DQ_KPI = 'Consistency'").collect()
    st.metric(label="Consistency", value=float(data_kpi[0]["DQ_SCORE"]), delta=float(data_kpi[0]["DQ_SCORE_DELTA"]))
with kpis_columns[3]:
    data_kpi = data.filter("DQ_KPI = 'Timeliness'").collect()
    st.metric(label="Timeliness", value=float(data_kpi[0]["DQ_SCORE"]), delta=float(data_kpi[0]["DQ_SCORE_DELTA"]))
with kpis_columns[4]:
    data_kpi = data.filter("DQ_KPI = 'Uniqueness'").collect()
    st.metric(label="Uniqueness", value=float(data_kpi[0]["DQ_SCORE"]), delta=float(data_kpi[0]["DQ_SCORE_DELTA"]))
with kpis_columns[5]:
    data_kpi = data.filter("DQ_KPI = 'Validity'").collect()
    st.metric(label="Validity", value=float(data_kpi[0]["DQ_SCORE"]), delta=float(data_kpi[0]["DQ_SCORE_DELTA"]))

# Last 30 days score
st.caption("Last 30 days Scoring:")
sql = f"""
    WITH source AS (
        SELECT * FROM dbt_dat.bi_dq_metrics
    ),
    dim_date AS (
        SELECT  DATEADD(DAY, -SEQ4(), CURRENT_DATE()) AS date
        FROM    TABLE(GENERATOR(ROWCOUNT=>30))
        WHERE   date <= (SELECT MAX(DATE_TRUNC('day', run_time)) FROM source)
    )
    SELECT      dim_date.date AS run_time,
                COALESCE(AVG((source.rows_processed - source.rows_failed) * 1.00 / NULLIF(source.rows_processed,0)), 0) * 100 AS score
    FROM        dim_date
    LEFT JOIN   source
        ON      source.run_time::DATE = dim_date.date
    GROUP BY    1
    ORDER BY    1 desc
"""
data = session.sql(sql)
with st.expander(label="Data"):
    st.dataframe(data, use_container_width=True)
st.line_chart(data=data.to_pandas(), x="RUN_TIME", y="SCORE", use_container_width=True)

# Source data
st.subheader("Source data")
st.caption("Sample 100 most recent logs")
sql = f"""
    SELECT      * 
    FROM        dq_issue_log 
    WHERE       no_of_records != 0
    ORDER BY    check_timestamp desc
    LIMIT       100
"""
data = session.sql(sql).collect()
st.dataframe(data, use_container_width=True)