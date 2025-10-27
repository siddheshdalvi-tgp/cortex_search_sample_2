import streamlit as st
from snowflake.snowpark import Session

# --- The connection Name is: "my_example_connection" ---
CONNECTION_NAME = "my_example_connection"

@st.cache_resource(show_spinner="Connecting to Snowflake...")
def create_session():
    # Use st.connection() to securely retrieve the configuration
    conn = st.connection(CONNECTION_NAME)
    
    # st.connection automatically handles the underlying connection based on the secrets.toml
    # For Snowpark, you can get the Snowpark Session object directly
    return conn.session

try:
    # 1. Create the session
    session = create_session()
    
    # 2. Test the connection
    result = session.sql("SELECT CURRENT_USER(), CURRENT_ACCOUNT()").collect()
    
    st.success("✅ Snowflake Snowpark connection successful!")
    st.write(f"Connected as: **{result[0]['CURRENT_USER()']}**")
    st.write(f"Current Account: **{result[0]['CURRENT_ACCOUNT()']}**")

except Exception as e:
    st.error("❌ Snowflake connection failed!")
    st.exception(e)

# Optional: Displaying data from a query
if session:
    st.header("Example Query")
    df = session.sql("SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_MONTHLY ORDER BY USAGE_DATE DESC LIMIT 5").to_pandas()
    st.dataframe(df)
