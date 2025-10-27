import streamlit as st
from snowflake.snowpark import Session
from snowflake.connector.errors import Error
#from snowflake.core import Root
#from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session

st.title("🔍 Snowflake Connectivity Test")

st.write("Attempting to connect to Snowflake...")

# Constants
DB = "CORTEX_DEMO_DB"
SCHEMA = "PUBLIC"
SERVICE = "CUSTOMER_COMMENT_SEARCH"
BASE_TABLE = "CORTEX_DEMO_DB.PUBLIC.CUSTOMER_DATA"

ARRAY_ATTRIBUTES = set()  # No array attributes in this case

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
    session.sql("ALTER WAREHOUSE CORTEX_WH RESUME IF SUSPENDED").collect()

    # session = Session.builder.configs(params).create()
    st.success("✅ Connected Successfully!")

    result = session.sql("SELECT CURRENT_VERSION()").collect()
    st.write("Snowflake Version:", result[0][0])

    def get_column_specification():
        #session = get_active_session()
        # session = create_session()
        result = session.sql(
            f"DESCRIBE CORTEX SEARCH SERVICE {DB}.{SCHEMA}.{SERVICE}"
        ).collect()[0]
    
        st.session_state.search_column = result.search_column
        st.session_state.columns = result.columns.split(",")
        st.session_state.attribute_columns = result.attribute_columns.split(",")


    def init_layout():
        st.title("Customer Comment Cortex Search")
        st.markdown(f"Using search service: `{DB}.{SCHEMA}.{SERVICE}`")
    
    
    def query_cortex_search_service(query, filter_object={}):
        filter_json = filter_object if filter_object else {}
        
        # 1. Escape single quotes in the filter JSON string to prevent SQL injection issues
        #    This is crucial because the JSON string is wrapped in single quotes in the SQL.
        filter_json_str = str(filter_json).replace("'", "''") 
        
        # 2. Use the correct table function name: SEARCH
        # 3. Use the correct syntax: TABLE(SEARCH(...))
        # 4. Apply the LIMIT clause outside the SEARCH function, as it's not an argument
        sql = f"""
            SELECT *
            FROM TABLE(SEARCH(
                '{DB}.{SCHEMA}.{SERVICE}',
                '{query}',
                PARSE_JSON('{filter_json_str}')
            ))
            LIMIT {st.session_state.limit} 
        """
        return session.sql(sql).collect()
    
    @st.cache_data
    def distinct_values_for_attribute(col_name):
        # session = get_active_session()
        values = session.sql(
            f"SELECT DISTINCT {col_name} AS VALUE FROM {BASE_TABLE}"
        ).collect()
        return [x["VALUE"] for x in values]
    
    
    def init_attribute_selection():
        st.session_state.attributes = {}
        for col in st.session_state.attribute_columns:
            st.session_state.attributes[col] = st.multiselect(
                label=f"Filter by {col}",
                options=distinct_values_for_attribute(col)
            )
    
    
    def init_search_input():
        st.session_state.query = st.text_input("Search Query")
    
    
    def init_limit_input():
        st.session_state.limit = st.number_input(
            "Number of results",
            min_value=1,
            value=5
        )
    
    
    def display_search_results(results):
        st.subheader("Results")
        for i, r in enumerate(results):
            r = dict(r)
            box = st.expander(f"Result {i + 1}", expanded=True)
    
            box.write(r[st.session_state.search_column])
    
            for col, val in sorted(r.items()):
                if col != st.session_state.search_column:
                    box.caption(f"{col}: {val}")
    
    
    def create_filter_object(attributes):
        clauses = []
        for col, values in attributes.items():
            if values:
                or_values = [{"@eq": {col: v}} for v in values]
                clauses.append({"@or": or_values})
    
        if clauses:
            return {"@and": clauses}
        return {}
    
    
    def main():
        init_layout()
        get_column_specification()
        init_attribute_selection()
        init_limit_input()
        init_search_input()
    
        if not st.session_state.query:
            return
    
        results = query_cortex_search_service(
            st.session_state.query,
            filter_object=create_filter_object(st.session_state.attributes)
        )
        display_search_results(results)
    
    
    if __name__ == "__main__":
        st.set_page_config(page_title="Cortex AI Search", layout="wide")
        main()


except Error as e:
    st.error(f"🚨 Snowflake Error: {e}")
except Exception as e:
    st.error(f"⚠️ General Error: {type(e).__name__}: {e}")













