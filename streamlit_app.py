# Import python packages
import streamlit as st
from snowflake.snowpark import Session

@st.cache_resource(show_spinner=False)
def create_session():
    # Access credentials from st.secrets
    secrets = st.secrets["snowflake"]
    
    connection_params = {
        "account": secrets["account"],
        "user": secrets["user"],
        "password": secrets["password"],
        "role": secrets["role"],
        "warehouse": secrets["warehouse"],
        "database": secrets["database"],
        "schema": secrets["schema"]
    }
    return Session.builder.configs(connection_params).create()

# The rest of your app logic remains the same
session = create_session()

# ✅ Test Connection
try:
    session.sql("SELECT CURRENT_USER(), CURRENT_ACCOUNT()").collect()
    st.success("✅ Snowflake connection successful (using st.secrets)!")
except Exception as e:
    st.error("❌ Snowflake connection failed!")
    st.error(e)
