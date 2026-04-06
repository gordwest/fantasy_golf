import pandas as pd
import streamlit as st
from helper import get_snowpark_session


def get_pool_entries():
    session = get_snowpark_session()
    return session.sql("""
        SELECT
            TEAM_NAME AS "Team Name",
            TIER_1_NAME AS "Tier1",
            TIER_2_NAME AS "Tier2",
            TIER_3_NAME AS "Tier3",
            TIER_4_NAME AS "Tier4",
            TIER_5_NAME AS "Tier5",
            TIER_6_NAME AS "Tier6",
            TIE_BREAKER AS "Tie Breaker",
            CREATED_BY AS "User"
        FROM PLAYER_ROSTERS
        ORDER BY 1
    """).to_pandas()


def render() -> None:
    session = get_snowpark_session()

    st.write("#### Masters Pool 2026 :golf:")
    st.write("###### Pool Entries")

    entries = get_pool_entries()

    if entries.empty:
        st.info("No entires yet... Be the first to submit your entry with the form below!")
    else:
        st.dataframe(entries, width="stretch", hide_index=True)

    st.divider()
    st.write("###### Team Selection")
    st.write("Build your team by selecting one player from each tier.")

    current_user = session.sql(
        "SELECT CURRENT_USER() AS CURRENT_USER"
    ).to_pandas().iloc[0]["CURRENT_USER"]
    existing_roster_count = session.sql(f"""
        SELECT COUNT(*) AS ENTRY_COUNT
        FROM PGA_MASTERS_2026.DATA.PLAYER_ROSTERS
        WHERE CREATED_BY = '{current_user}'
    """).to_pandas().iloc[0]["ENTRY_COUNT"]
    has_existing_roster = int(existing_roster_count) > 0

    if has_existing_roster:
        st.warning("You already have an entry in this pool. Submitting again will overwrite your existing entry.")

    name_column, tie_breaker_column = st.columns(2)
    team_name = name_column.text_input("Team Name")
    tie_breaker = tie_breaker_column.number_input("Tie Breaker", value=288)

    field = session.sql("""
        SELECT PLAYER, PLAYERID, OWGR_RANK AS "RANK", TIER
        FROM PGA_MASTERS_2026.DATA.TOURNAMENT_FIELD_RANKING
        ORDER BY TIER, RELATIVE_RANK
    """).to_pandas()

    selections = {}
    tier_values = sorted(field["TIER"].unique())
    for tier_group in [tier_values[:3], tier_values[3:6]]:
        columns = st.columns(3)
        for column, tier in zip(columns, tier_group):
            tier_players = field[field["TIER"] == tier]
            options = tier_players.apply(
                lambda row: f"{row['PLAYER']} (OWGR: {int(row['RANK']) if pd.notna(row['RANK']) else 'None'})",
                axis=1,
            ).tolist()
            with column:
                choice = st.selectbox(
                    f"Tier {tier}",
                    options,
                    index=None,
                    placeholder="Select a player...",
                )
            if choice:
                selected_row = tier_players.iloc[options.index(choice)]
                selections[tier] = {
                    "name": selected_row["PLAYER"],
                    "id": str(selected_row["PLAYERID"]),
                }

    submit_label = "Update Entry" if has_existing_roster else "Submit Entry"

    if st.button(submit_label, type="primary", disabled=(len(selections) != 6 or not team_name)):
        if has_existing_roster:
            session.sql(f"""
                DELETE FROM PGA_MASTERS_2026.DATA.PLAYER_ROSTERS
                WHERE CREATED_BY = '{current_user}'
            """).collect()

        session.sql(f"""
            INSERT INTO PGA_MASTERS_2026.DATA.PLAYER_ROSTERS
            (TEAM_NAME, TIE_BREAKER,
                TIER_1_NAME, TIER_1_ID,
                TIER_2_NAME, TIER_2_ID,
                TIER_3_NAME, TIER_3_ID,
                TIER_4_NAME, TIER_4_ID,
                TIER_5_NAME, TIER_5_ID,
                TIER_6_NAME, TIER_6_ID)
            VALUES (
                '{team_name}', {tie_breaker},
                '{selections[1]["name"]}', {selections[1]["id"]},
                '{selections[2]["name"]}', {selections[2]["id"]},
                '{selections[3]["name"]}', {selections[3]["id"]},
                '{selections[4]["name"]}', {selections[4]["id"]},
                '{selections[5]["name"]}', {selections[5]["id"]},
                '{selections[6]["name"]}', {selections[6]["id"]}
            )
        """).collect()
        get_pool_entries.clear()
        success_message = (
            f"Team '{team_name}' updated successfully!"
            if has_existing_roster
            else f"Team '{team_name}' submitted successfully!"
        )
        st.success(success_message)
        st.balloons()
        st.rerun()

    if not team_name or len(selections) != 6:
        st.caption("Select a team name and one player from each tier to submit.")
