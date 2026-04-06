import requests
from snowflake.snowpark import Session
from config import get_rapidapi_config, get_snowflake_config

RAPIDAPI_CONFIG = get_rapidapi_config()
BASE_URL = RAPIDAPI_CONFIG["base_url"]
RAPIDAPI_HOST = RAPIDAPI_CONFIG["host"]
TARGET_DATABASE = "PGA_MASTERS_2026"
TARGET_SCHEMA = "DATA"


def build_headers() -> dict[str, str]:
    return {
        "x-rapidapi-key": RAPIDAPI_CONFIG["api_key"],
        "x-rapidapi-host": RAPIDAPI_HOST,
        "Content-Type": "application/json",
    }


def build_snowflake_connection_parameters(
    *,
    database: str = TARGET_DATABASE,
    schema: str = TARGET_SCHEMA,
) -> dict[str, str]:
    return get_snowflake_config(database=database, schema=schema)


def create_snowpark_session(
    *,
    database: str = TARGET_DATABASE,
    schema: str = TARGET_SCHEMA,
) -> Session:
    connection_parameters = build_snowflake_connection_parameters(database=database, schema=schema)
    return Session.builder.configs(connection_parameters).create()


def fetch_json(
    endpoint: str,
    *,
    params: dict[str, str],
    base_url: str = BASE_URL,
) -> dict:
    response = requests.get(
        f"{base_url}/{endpoint}",
        headers=build_headers(),
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def write_dataframe_to_snowflake(
    dataframe,
    *,
    table_name: str,
    database: str = TARGET_DATABASE,
    schema: str = TARGET_SCHEMA,
    overwrite: bool = True,
) -> None:
    session = create_snowpark_session(database=database, schema=schema)

    try:
        session.write_pandas(
            dataframe,
            table_name=table_name,
            database=database,
            schema=schema,
            auto_create_table=False,
            overwrite=overwrite,
            quote_identifiers=False,
        )
    finally:
        session.close()
