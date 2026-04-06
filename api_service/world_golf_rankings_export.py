from typing import Any
import pandas as pd

from export_shared import (
    BASE_URL,
    TARGET_DATABASE,
    TARGET_SCHEMA,
    fetch_json,
    write_dataframe_to_snowflake,
)

WORLD_GOLF_RANKING_STAT_ID = "186"
TARGET_TABLE = "WORLD_GOLF_RANKINGS"


def parse_rank(raw_rank: Any) -> int | None:
    if isinstance(raw_rank, dict):
        raw_rank = raw_rank.get("$numberInt") or raw_rank.get("$numberLong")

    if raw_rank in (None, ""):
        return None

    return int(raw_rank)


def fetch_world_rankings(*, year: str, base_url: str = BASE_URL) -> list[dict[str, Any]]:
    response_json = fetch_json(
        "stats",
        params={"year": year, "statId": WORLD_GOLF_RANKING_STAT_ID},
        base_url=base_url,
    )
    return response_json["rankings"]


def build_world_golf_rankings_df(*, year: str) -> pd.DataFrame:
    rankings = fetch_world_rankings(year=year)

    rows = []
    for golfer in rankings:
        rows.append(
            {
                "PLAYERID": str(golfer.get("playerId")).strip() or None,
                "PLAYER": golfer.get("fullName")
                or " ".join(part for part in [golfer.get("firstName"), golfer.get("lastName")] if part),
                "RANK": parse_rank(golfer.get("rank")),
            }
        )

    return pd.DataFrame(rows)

def write_world_golf_rankings_to_snowflake(
    rankings_df: pd.DataFrame,
    *,
    table_name: str = TARGET_TABLE,
    database: str = TARGET_DATABASE,
    schema: str = TARGET_SCHEMA,
    overwrite: bool = True,
) -> None:
    write_dataframe_to_snowflake(
        rankings_df,
        table_name=table_name,
        database=database,
        schema=schema,
        overwrite=overwrite,
    )


def main() -> None:

    rankings_df = build_world_golf_rankings_df(
        year="2026",
    )

    write_world_golf_rankings_to_snowflake(rankings_df)
    # print(rankings_df)
    print(f"Loaded {len(rankings_df)} rows into {TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}.")


if __name__ == "__main__":
    main()
