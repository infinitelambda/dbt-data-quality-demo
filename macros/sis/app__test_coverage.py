import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import avg

st.set_page_config(layout="wide")

# Base
session = get_active_session()

# Header
st.title("Data Testing Coverage")
st.write("""
Source data captured by [dq-tools](https://infinitelambda.github.io/dq-tools/)
""")

st.caption("Today Coverage:")
sql = f"""
    WITH source AS (
        SELECT * FROM dbt_dat.test_coverage
    ),
    today AS (
        SELECT  DATE_TRUNC('day', check_timestamp) AS run_date,
                CAST(AVG(coverage_pct) as FLOAT) AS test_coverage,
                CAST(AVG(test_to_column_ratio) as FLOAT) AS test_to_column
        FROM    source
        WHERE   DATE_TRUNC('day', check_timestamp) = (SELECT MAX(DATE_TRUNC('day', check_timestamp)) FROM source)
        GROUP BY ALL
    ),
    yesterday AS (
        SELECT  CAST(AVG(coverage_pct) as FLOAT) AS test_coverage,
                CAST(AVG(test_to_column_ratio) as FLOAT) AS test_to_column
        FROM    source
        WHERE   DATE_TRUNC('day', check_timestamp) = (
                    SELECT  MAX(DATE_TRUNC('day', check_timestamp)) 
                    FROM    source 
                    WHERE   check_timestamp < (SELECT MIN(run_date) FROM today)
                )
        GROUP BY ALL
    )
    SELECT      today.test_coverage,
                COALESCE(today.test_coverage - yesterday.test_coverage, 0) AS test_coverage_delta,
                today.test_to_column,
                COALESCE(today.test_to_column - yesterday.test_to_column, 0) AS test_to_column_delta
    FROM        today
    CROSS JOIN   yesterday
    
"""
data = session.sql(sql).collect()

card_columns = st.columns(2)
with card_columns[0]:
    st.metric(label="Coverage Percentage", value=float(data[0]["TEST_COVERAGE"]), delta=float(data[0]["TEST_COVERAGE_DELTA"]))
with card_columns[1]:
    st.metric(label="Test to Column", value=data[0]["TEST_TO_COLUMN"], delta=data[0]["TEST_TO_COLUMN_DELTA"])
with st.expander(label="Data"):
    st.dataframe(data, use_container_width=True)
    
# Last 7 days coverage
st.caption("Last 7 days Coverage:")
sql = f"""
    WITH source AS (
        SELECT * FROM dbt_dat.test_coverage
    ),
    dim_date AS (
        SELECT  DATEADD(DAY, -SEQ4(), CURRENT_DATE()) AS date
        FROM    TABLE(GENERATOR(ROWCOUNT=>7))
        WHERE   date <= (SELECT MAX(DATE_TRUNC('day', check_timestamp)) FROM source)
    )
    SELECT      dim_date.date AS run_time,
                CAST(COALESCE(AVG(source.coverage_pct), 0) AS FLOAT) AS test_coverage,
                CAST(70 AS FLOAT) AS target
    FROM        dim_date
    LEFT JOIN   source
        ON      source.check_timestamp::DATE = dim_date.date
    GROUP BY    1
    ORDER BY    1 desc
"""
data = session.sql(sql)
st.line_chart(data=data, x="RUN_TIME", y=["TEST_COVERAGE", "TARGET"], use_container_width=True)
with st.expander(label="Data"):
    st.dataframe(data, use_container_width=True)

# Source data
st.subheader("Source data")
with st.expander("Sample 100 most recent logs"):
    sql = f"""
        SELECT      * 
        FROM        dq_issue_log 
        WHERE       no_of_records != 0
        ORDER BY    check_timestamp desc
        LIMIT       100
    """
    data = session.sql(sql).collect()
    st.dataframe(data, use_container_width=True)