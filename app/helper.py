from snowflake.snowpark import Session
import streamlit as st
from snowflake.snowpark.context import get_active_session

TARGET_DATABASE = "PGA_MASTERS_2026"
TARGET_SCHEMA = "DATA"


def build_snowflake_connection_parameters(
    *,
    database: str = TARGET_DATABASE,
    schema: str = TARGET_SCHEMA,
) -> dict[str, str]:
    snowflake_secrets = st.secrets["snowflake"]

    config = {
        "account": snowflake_secrets["account"],
        "user": snowflake_secrets["user"],
        "password": snowflake_secrets["password"],
        "warehouse": snowflake_secrets["warehouse"],
        "database": database or snowflake_secrets["database"],
        "schema": schema or snowflake_secrets["schema"],
    }

    missing_parameters = [key.upper() for key, value in config.items() if not value]
    if missing_parameters:
        missing_list = ", ".join(missing_parameters)
        raise ValueError(f"Missing required Streamlit Snowflake secrets: {missing_list}")

    return config


def create_snowpark_session(
    *,
    database: str = TARGET_DATABASE,
    schema: str = TARGET_SCHEMA,
) -> Session:
    connection_parameters = build_snowflake_connection_parameters(database=database, schema=schema)
    return Session.builder.configs(connection_parameters).create()


def create_deployed_snowpark_session() -> Session:
    return get_active_session()


def get_snowpark_session():
    if "snowpark_session" not in st.session_state:
        try:
            st.session_state.snowpark_session = create_deployed_snowpark_session()
        except Exception:
            st.session_state.snowpark_session = create_snowpark_session()

    return st.session_state.snowpark_session
