import os
import tomllib
from pathlib import Path
from __future__ import annotations


ROOT_DIR = Path(__file__).resolve().parent
SECRETS_FILE = ROOT_DIR / ".secrets.toml"


def load_local_secrets() -> dict:
    if not SECRETS_FILE.exists():
        return {}

    with SECRETS_FILE.open("rb") as secrets_file:
        return tomllib.load(secrets_file)


LOCAL_SECRETS = load_local_secrets()


def _get_nested_secret(section: str, key: str, env_var: str, default: str | None = None) -> str | None:
    section_values = LOCAL_SECRETS.get(section, {})
    value = section_values.get(key) or os.getenv(env_var) or default
    return value


def get_snowflake_config(
    *,
    database: str | None = None,
    schema: str | None = None,
) -> dict[str, str]:
    config = {
        "account": _get_nested_secret("snowflake", "account", "SNOWFLAKE_ACCOUNT"),
        "user": _get_nested_secret("snowflake", "user", "SNOWFLAKE_USER"),
        "password": _get_nested_secret("snowflake", "password", "SNOWFLAKE_PASSWORD"),
        "warehouse": _get_nested_secret("snowflake", "warehouse", "SNOWFLAKE_WAREHOUSE"),
        "database": database or _get_nested_secret("snowflake", "database", "SNOWFLAKE_DATABASE"),
        "schema": schema or _get_nested_secret("snowflake", "schema", "SNOWFLAKE_SCHEMA"),
    }

    missing_parameters = [key.upper() for key, value in config.items() if not value]
    if missing_parameters:
        missing_list = ", ".join(missing_parameters)
        raise ValueError(f"Missing required Snowflake configuration: {missing_list}")

    return config


def get_rapidapi_config() -> dict[str, str]:
    config = {
        "base_url": _get_nested_secret(
            "rapidapi",
            "base_url",
            "RAPIDAPI_BASE_URL",
            default="https://live-golf-data.p.rapidapi.com",
        ),
        "host": _get_nested_secret(
            "rapidapi",
            "host",
            "RAPIDAPI_HOST",
            default="live-golf-data.p.rapidapi.com",
        ),
        "api_key": _get_nested_secret("rapidapi", "api_key", "RAPIDAPI_KEY"),
    }

    missing_parameters = [key.upper() for key, value in config.items() if not value]
    if missing_parameters:
        missing_list = ", ".join(missing_parameters)
        raise ValueError(f"Missing required RapidAPI configuration: {missing_list}")

    return config
