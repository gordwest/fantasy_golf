import pandas as pd

from export_shared import (
    BASE_URL,
    TARGET_DATABASE,
    TARGET_SCHEMA,
    fetch_json,
    write_dataframe_to_snowflake,
)

TARGET_TABLE = "TOURNAMENT_FIELD"


def fetch_tournament_players(
    *,
    year: str,
    org_id: str,
    tourn_id: str,
    base_url: str = BASE_URL,
) -> list[dict]:
    response_json = fetch_json(
        "tournament",
        params={"orgId": org_id, "tournId": tourn_id, "year": year},
        base_url=base_url,
    )
    return response_json["players"]


def build_tournament_field_df(
    *,
    year: str,
    org_id: str,
    tourn_id: str,
) -> pd.DataFrame:
    tournament_players = fetch_tournament_players(
        year=year,
        org_id=org_id,
        tourn_id=tourn_id,
    )

    rows = []
    for golfer in tournament_players:
        full_name = " ".join(part for part in [golfer.get("firstName"), golfer.get("lastName")] if part)
        rows.append(
            {
                "PLAYERID": str(golfer.get("playerId")).strip() or None,
                "PLAYER": full_name,
                "FIRSTNAME": golfer.get("firstName"),
                "LASTNAME": golfer.get("lastName"),
                "IS_AMATEUR": golfer.get("isAmateur"),
            }
        )

    return pd.DataFrame(rows)

def write_tournament_field_to_snowflake(
    tournament_field_df: pd.DataFrame,
    *,
    table_name: str = TARGET_TABLE,
    database: str = TARGET_DATABASE,
    schema: str = TARGET_SCHEMA,
    overwrite: bool = True,
) -> None:
    write_dataframe_to_snowflake(
        tournament_field_df,
        table_name=table_name,
        database=database,
        schema=schema,
        overwrite=overwrite,
    )


def main() -> None:

    tournament_field_df = build_tournament_field_df(
        year="2026",
        org_id="1",
        # tourn_id="014", # masters
        tourn_id="041",
    )

    write_tournament_field_to_snowflake(tournament_field_df)
    # print(tournament_field_df)
    print(f"Loaded {len(tournament_field_df)} rows into {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}.")


if __name__ == "__main__":
    main()
