# Import python packages
import streamlit as st
from snowflake.core import Root
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session

# Hardcoded credentials instead of secrets
account = "OBIMSEL-PZ16899"
user = "SIDDHESH3PILLARGLOBAL"
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

# Constants
DB = DATABASE
SCHEMA = SCHEMA
SERVICE = "CUSTOMER_COMMENT_SEARCH"
BASE_TABLE = f"{DB}.{SCHEMA}.CUSTOMER_DATA"

ARRAY_ATTRIBUTES = set()  # No array attributes in this case

session.sql("ALTER WAREHOUSE CORTEX_WH RESUME IF SUSPENDED").collect()


def get_column_specification():
    result = session.sql(
        f"DESC CORTEX SEARCH SERVICE {DB}.{SCHEMA}.{SERVICE}"
    ).collect()[0]

    st.session_state.search_column = result.search_column
    st.session_state.columns = result.columns.split(",")
    st.session_state.attribute_columns = result.attribute_columns.split(",")


def init_layout():
    st.title("Customer Comment Cortex Search")
    st.markdown(f"Using search service: `{DB}.{SCHEMA}.{SERVICE}`")


def query_cortex_search_service(query, filter_object={}):
    session = get_active_session()
    cortex_service = (
        Root(session)
        .databases[DB]
        .schemas[SCHEMA]
        .cortex_search_services[SERVICE]
    )

    docs = cortex_service.search(
        query,
        columns=st.session_state.columns,
        filter=filter_object,
        limit=st.session_state.limit
    )
    return docs.results


@st.cache_data
def distinct_values_for_attribute(col_name):
    session = get_active_session()
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
            clauses.append({"





