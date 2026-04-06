from typing import Any

import pandas as pd

from export_shared import (
    BASE_URL,
    TARGET_DATABASE,
    TARGET_SCHEMA,
    fetch_json,
    write_dataframe_to_snowflake,
)

TARGET_TABLE = "LEADERBOARD"


def build_round_score_fields(row: dict[str, Any]) -> dict[str, Any]:
    round_score_fields: dict[str, Any] = {
        "R1": None,
        "R2": None,
        "R3": None,
        "R4": None,
    }

    for index, round_data in enumerate(row.get("rounds") or [], start=1):
        round_score_fields[f"R{index}"] = round_data.get("scoreToPar") or "0"

    if row.get("status") == 'active':
        current_round = row.get("currentRound").get("$numberInt")
        current_round_score = row.get("currentRoundScore")

        try:
            current_round_number = int(current_round)
        except (TypeError, ValueError):
            current_round_number = None

        if current_round_number in range(1, 5) and current_round_score not in (None, ""):
            round_score_fields[f"R{current_round_number}"] = current_round_score

    return round_score_fields


def fetch_leaderboard(
    *,
    year: str,
    org_id: str,
    tourn_id: str,
    base_url: str = BASE_URL,
) -> list[dict[str, Any]]:
    response_json = fetch_json(
        "leaderboard",
        params={"orgId": org_id, "tournId": tourn_id, "year": year},
        base_url=base_url,
    )
    return response_json["leaderboardRows"]


def build_leaderboard_records(leaderboard_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    leaderboard_records: list[dict[str, Any]] = []

    for row in leaderboard_rows:
        leaderboard_records.append(
            {
                "playerId": row.get("playerId"),
                "status": row.get("status"),
                "position": row.get("position"),
                "total": row.get("total"),
                **build_round_score_fields(row),
            }
        )

    return leaderboard_records


def build_leaderboard_df(
    *,
    year: str,
    org_id: str,
    tourn_id: str,
) -> pd.DataFrame:
    leaderboard_rows = fetch_leaderboard(
        year=year,
        org_id=org_id,
        tourn_id=tourn_id,
    )
    leaderboard_records = build_leaderboard_records(leaderboard_rows)
    return pd.DataFrame(leaderboard_records)

def write_leaderboard_to_snowflake(
    leaderboard_df: pd.DataFrame,
    *,
    table_name: str = TARGET_TABLE,
    database: str = TARGET_DATABASE,
    schema: str = TARGET_SCHEMA,
    overwrite: bool = True,
) -> None:
    write_dataframe_to_snowflake(
        leaderboard_df,
        table_name=table_name,
        database=database,
        schema=schema,
        overwrite=overwrite,
    )


def main() -> None:

    leaderboard_df = build_leaderboard_df(
        year="2026",
        org_id="1",
        # tourn_id="014",
        tourn_id="041",
    )

    write_leaderboard_to_snowflake(leaderboard_df)
    # print(leaderboard_df)
    print(f"Loaded {len(leaderboard_df)} rows into {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}.")


if __name__ == "__main__":
    main()
