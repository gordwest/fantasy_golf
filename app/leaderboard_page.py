import pandas as pd
import streamlit as st
from helper import get_snowpark_session


def style_tournament_leaderboard_row(row: pd.Series) -> list[str]:
    status = str(row.get("STATUS", "")).strip().lower()

    if status in ("active", "complete"):
        background = "#1F4D2E"
    elif status in {"cut", "wd"}:
        background = "#5A1E24"
    else:
        background = ""

    return [f"background-color: {background}" if background else "" for _ in row]


def format_tournament_value(value):
    if pd.isna(value):
        return ""

    if isinstance(value, (int, float)) and float(value).is_integer():
        return f"{int(value)}"

    return value


def render() -> None:
    session = get_snowpark_session()

    st.write("#### Masters Pool 2026 :golf:")
    st.write("###### Pool Leaderboard")

    rosters = session.sql("SELECT * FROM PGA_MASTERS_2026.DATA.PLAYER_ROSTERS").to_pandas()

    if rosters.empty:
        st.info("No teams have been created yet... Head to the Teams page to join!")
    else:
        team_scores = []
        for _, row in rosters.iterrows():
            player_ids = [
                row["TIER_1_ID"], row["TIER_2_ID"], row["TIER_3_ID"],
                row["TIER_4_ID"], row["TIER_5_ID"], row["TIER_6_ID"],
            ]
            ids_str = ",".join([f"'{pid}'" for pid in player_ids])
            scores = session.sql(f"""
                SELECT PLAYERID, TOTAL
                FROM PGA_MASTERS_2026.DATA.LEADERBOARD
                WHERE PLAYERID IN ({ids_str})
            """).to_pandas()
            scores["TOTAL"] = pd.to_numeric(scores["TOTAL"], errors="coerce")
            total = scores["TOTAL"].sum()
            team_scores.append(
                {
                    "TEAM": row["TEAM_NAME"],
                    "SCORE": int(total) if pd.notna(total) else 0,
                }
            )

        team_df = pd.DataFrame(team_scores).sort_values("SCORE")
        team_df.index = range(1, len(team_df) + 1)
        team_df.index.name = "RANK"
        st.dataframe(team_df, width="stretch")

    st.divider()
    st.write("###### Tournament Leaderboard")
    leaderboard = session.sql("""
        SELECT
            POSITION,
            PLAYER,
            STATUS,
            TOTAL,
            R1,
            R2,
            R3,
            R4
        FROM PGA_MASTERS_2026.DATA.LEADERBOARD_DETAILS
        ORDER BY TOTAL ASC
    """).to_pandas()
    styled_leaderboard = (
        leaderboard.style
        .apply(style_tournament_leaderboard_row, axis=1)
        .format(format_tournament_value)
    )
    st.dataframe(styled_leaderboard, width="stretch", hide_index=True)
