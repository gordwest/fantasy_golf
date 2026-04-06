from __future__ import annotations
import os
import tomllib
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent
SECRETS_FILE = ROOT_DIR / ".secrets.toml"


def load_local_secrets() -> dict:
    if not SECRETS_FILE.exists():
        return {}

    with SECRETS_FILE.open("rb") as secrets_file:
        return tomllib.load(secrets_file)


LOCAL_SECRETS = load_local_secrets()


def _get_nested_secret(
    section: str,
    key: str,
    env_var: str,
    *,
    env_aliases: tuple[str, ...] = (),
    default: str | None = None,
) -> str | None:
    # Support either a flat TOML layout or nested sections like [rapidapi] / [snowflake].
    section_values = LOCAL_SECRETS.get(section, {})
    if isinstance(section_values, dict):
        nested_value = section_values.get(key)
        if nested_value:
            return nested_value

    flat_value = LOCAL_SECRETS.get(env_var)
    if flat_value:
        return flat_value

    for candidate in (env_var, *env_aliases):
        env_value = os.getenv(candidate)
        if env_value:
            return env_value

    return default


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
            env_aliases=("BASE_URL",),
            default="https://live-golf-data.p.rapidapi.com",
        ),
        "host": _get_nested_secret(
            "rapidapi",
            "host",
            "RAPIDAPI_HOST",
            env_aliases=("HOST",),
            default="live-golf-data.p.rapidapi.com",
        ),
        "api_key": _get_nested_secret(
            "rapidapi",
            "api_key",
            "RAPIDAPI_KEY",
            env_aliases=("API_KEY",),
        ),
    }

    missing_parameters = []
    if not config["api_key"]:
        missing_parameters.append("api_key (set RAPIDAPI_KEY or API_KEY)")
    if not config["host"]:
        missing_parameters.append("host (set RAPIDAPI_HOST or HOST)")
    if not config["base_url"]:
        missing_parameters.append("base_url (set RAPIDAPI_BASE_URL or BASE_URL)")

    if missing_parameters:
        missing_list = ", ".join(missing_parameters)
        raise ValueError(f"Missing required RapidAPI configuration: {missing_list}")

    return config
