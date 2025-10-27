import streamlit as st
from snowflake.snowpark import Session
from snowflake.connector.errors import Error

@st.cache_data
def create_session():
    try:
        secrets = st.secrets["snowflake"]
        connection_params = {
            "account": secrets.get("account"),
            "user": secrets.get("user"),
            "password": secrets.get("password"),
            "role": secrets.get("role"),
            "warehouse": secrets.get("warehouse"),
            "database": secrets.get("database"),
            "schema": secrets.get("schema")
        }
        session = Session.builder.configs(connection_params).create()
        return session

    except Error as e:
        st.error(f"Snowflake Connection Error: {e}", icon="🚨")
        raise
    except Exception as e:
        st.error(f"General Error: {type(e).__name__}: {e}", icon="⚠️")
        raise
