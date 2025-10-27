import streamlit as st
from snowflake.snowpark import Session
from snowflake.connector.errors import Error

st.title("🔍 Snowflake Connectivity Test")

st.write("Attempting to connect to Snowflake...")

try:
    sf = st.secrets["snowflake"]
    params = {
        "account": sf.get("account"),
        "user": sf.get("user"),
        "password": sf.get("password"),
        "role": sf.get("role"),
        "warehouse": sf.get("warehouse"),
        "database": sf.get("database"),
        "schema": sf.get("schema")
    }

    session = Session.builder.configs(params).create()
    st.success("✅ Connected Successfully!")

    result = session.sql("SELECT CURRENT_VERSION()").collect()
    st.write("Snowflake Version:", result[0][0])

except Error as e:
    st.error(f"🚨 Snowflake Error: {e}")
except Exception as e:
    st.error(f"⚠️ General Error: {type(e).__name__}: {e}")
