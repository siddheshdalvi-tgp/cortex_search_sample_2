# Import python packages
import streamlit as st
from snowflake.snowpark import Session

# Hardcoded credentials instead of secrets
ACCOUNT = "OBIMSEL-PZ16899"
USER = "SIDDHESH3PILLARGLOBAL"
PASSWORD = "QwertyQwerty@456"
ROLE = "CORTEX_APP_ROLE"
WAREHOUSE = "CORTEX_WH"
DATABASE = "CORTEX_DEMO_DB"
SCHEMA = "PUBLIC"

@st.cache_resource(show_spinner=False)
def create_session():
    connection_params = {
        "account": ACCOUNT,
        "user": USER,
        "password": PASSWORD,
        "role": ROLE,
        "warehouse": WAREHOUSE,
        "database": DATABASE,
        "schema": SCHEMA
    }
    return Session.builder.configs(connection_params).create()

session = create_session()

# ✅ Test Connection
try:
    session.sql("SELECT CURRENT_USER(), CURRENT_ACCOUNT()").collect()
    st.success("✅ Snowflake connection successful!")
except Exception as e:
    st.error("❌ Snowflake connection failed!")
    st.error(e)

