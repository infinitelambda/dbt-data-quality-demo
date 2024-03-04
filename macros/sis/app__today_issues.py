import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide")

# Base
session = get_active_session()

# Header
st.title("Today Data Issues")
st.write("""
Source data captured by [dq-tools](https://infinitelambda.github.io/dq-tools/)
""")

st.caption("")
sql = f"""
    WITH source AS (
        SELECT  *
                ,CASE WHEN no_of_records_failed = 0 THEN 0 ELSE 1 END status
                ,ROW_NUMBER() OVER (PARTITION BY test_unique_id ORDER BY check_timestamp DESC) as run_number
        FROM    dq_issue_log
    ),
    today AS (
        SELECT  test_unique_id,
                kpi_category,
                dq_issue_type,
                status,
                DATE_TRUNC('day', check_timestamp) AS run_date,
                CAST(AVG(no_of_records_failed) AS FLOAT) AS avg_rows_failed,
                CAST(SUM(CASE WHEN run_number = 1 THEN no_of_records_failed ELSE 0 END) AS FLOAT) AS last_rows_failed
        FROM    source
        WHERE   DATE_TRUNC('day', check_timestamp) = (SELECT MAX(DATE_TRUNC('day', check_timestamp)) FROM source)
        GROUP BY ALL
    ),
    yesterday AS (
        SELECT  test_unique_id,
                kpi_category,
                dq_issue_type,
                status,
                CAST(AVG(no_of_records_failed) AS FLOAT) AS avg_rows_failed
        FROM    source
        WHERE   DATE_TRUNC('day', check_timestamp) = (
                    SELECT  MAX(DATE_TRUNC('day', check_timestamp)) 
                    FROM    source 
                    WHERE   check_timestamp < (SELECT MIN(run_date) FROM today)
                )
        GROUP BY ALL
    ),
    final AS (
        SELECT      today.test_unique_id,
                    today.kpi_category,
                    today.dq_issue_type,
                    COALESCE(today.avg_rows_failed, 0) AS avg_rows_failed,
                    COALESCE(today.avg_rows_failed - COALESCE(yesterday.avg_rows_failed, 0), 0) AS avg_rows_failed_delta,
                    COALESCE(today.last_rows_failed, 0) AS last_rows_failed,
                    CASE
                        WHEN COALESCE(yesterday.avg_rows_failed, 0) = 0 THEN '❗'
                        WHEN avg_rows_failed_delta > 0 THEN '⬆'
                        WHEN avg_rows_failed_delta < 0 THEN '⬇'
                        ELSE '➖'
                    END  || '(' || avg_rows_failed_delta::NUMBER || ')' AS indicator,
                    CASE
                        WHEN yesterday.avg_rows_failed = 0 THEN 0
                        ELSE ABS(avg_rows_failed)
                    END AS priortiy
        FROM        today
        LEFT JOIN   yesterday USING (test_unique_id)
        WHERE       today.status > 0 OR yesterday.status > 0
    )
    SELECT  test_unique_id,
            kpi_category,
            indicator,
            avg_rows_failed,
            last_rows_failed
    FROM    final
    ORDER BY priortiy
    
"""
data = session.sql(sql)
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
