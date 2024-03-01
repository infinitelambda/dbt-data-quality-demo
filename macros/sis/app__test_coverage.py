import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide")

# Base
session = get_active_session()

# Header
st.title("Data Testing Coverage")
st.write("""
Source data captured by [dq-tools](https://infinitelambda.github.io/dq-tools/)
""")


st.subheader("")
st.caption("")
sql = f"""
    
"""
data = session.sql(sql).collect()
st.dataframe(data, use_container_width=True)
