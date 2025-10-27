# ⚠️ WARNING: DO NOT USE THIS FOR PRODUCTION. This file now demonstrates 
# how to use 'externalbrowser' authentication, which is safer than hardcoding a password.
import streamlit as st
from snowflake.snowpark import Session

st.title("Snowpark Connection Test (Hardcoded Credentials - Using External Browser)")

# Hardcoded credentials
ACCOUNT = "ld31269.ap-south-1"
USER = "SIDDHESH3PILLARGLOBAL"
# ⚠️ Removed PASSWORD for externalbrowser authentication (It is not needed)
ROLE = "CORTEX_APP_ROLE"
WAREHOUSE = "CORTEX_WH"
DATABASE = "CORTEX_DEMO_DB"
SCHEMA = "PUBLIC"

@st.cache_resource(show_spinner="Connecting to Snowflake...")
def create_hardcoded_session():
    connection_params = {
        "account": ACCOUNT,
        "user": USER,
        # 💡 Add the authenticator parameter for secure login flow
        "authenticator": "externalbrowser",
        "role": ROLE,
        "warehouse": WAREHOUSE,
        "database": DATABASE,
        "schema": SCHEMA
    }
    # This will work, but is unsafe
    return Session.builder.configs(connection_params).create()

# --- Main Logic ---

st.info("A browser window may open asking you to log into Snowflake. Please authorize the connection there.")

try:
    session = create_hardcoded_session()
    session.sql("SELECT CURRENT_USER()").collect()
    st.success("✅ Snowflake connection successful with external browser authentication!")
    st.warning("⚠️ SECURITY NOTE: The credentials are still hardcoded, but external browser authentication is generally more robust than a static password.")
except Exception as e:
    st.error("❌ Snowflake connection failed!")
    st.exception(e)
