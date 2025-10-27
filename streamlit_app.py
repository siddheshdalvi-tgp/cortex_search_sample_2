# ⚠️ WARNING: DO NOT USE THIS FOR PRODUCTION. THIS IS FOR DEMONSTRATION ONLY.
import streamlit as st
from snowflake.snowpark import Session

st.title("Snowpark Connection Test (Hardcoded Credentials)")

# Hardcoded credentials
ACCOUNT = "ld31269.ap-south-1"
USER = "SIDDHESH3PILLARGLOBAL"
PASSWORD = "QwertyQwerty@456" # EXPOSED
ROLE = "CORTEX_APP_ROLE"
WAREHOUSE = "CORTEX_WH"
DATABASE = "CORTEX_DEMO_DB"
SCHEMA = "PUBLIC"

@st.cache_resource(show_spinner="Connecting to Snowflake...")
def create_hardcoded_session():
    connection_params = {
        "account": ACCOUNT,
        "user": USER,
        "password": PASSWORD,
        "role": ROLE,
        "warehouse": WAREHOUSE,
        "database": DATABASE,
        "schema": SCHEMA
    }
    # This will work, but is unsafe
    return Session.builder.configs(connection_params).create()

# --- Main Logic ---

session = create_hardcoded_session()

try:
    session.sql("SELECT CURRENT_USER()").collect()
    st.success("✅ Snowflake connection successful with hardcoded credentials!")
    st.warning("⚠️ SECURITY WARNING: Your password is visible in the source code. Please switch back to using `st.secrets`.")
except Exception as e:
    st.error("❌ Snowflake connection failed!")
    st.exception(e)
