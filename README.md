# Fantasy Golf

This repo has two parts:

- `app/`: the Streamlit app
- `api_service/`: scripts that fetch external golf data and load Snowflake tables

## Secrets

The repo uses two secret locations:

- `app/.streamlit/secrets.toml` for the Streamlit app
- `.secrets.toml` for the external `api_service/` scripts

To set up a new environment:

1. Copy `app/.streamlit/secrets.example.toml` to `app/.streamlit/secrets.toml`
2. Add your Snowflake credentials for the app
3. Copy `.secrets.example.toml` to `.secrets.toml`
4. Add your Snowflake and RapidAPI credentials for the export scripts
5. Run the app or export scripts as usual

The code also supports environment variable overrides for:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `RAPIDAPI_BASE_URL`
- `RAPIDAPI_HOST`
- `RAPIDAPI_KEY`
