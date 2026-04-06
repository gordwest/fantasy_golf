import streamlit as st
from helper import get_snowpark_session


def render() -> None:
    session = get_snowpark_session()

    st.write("#### Masters Pool 2026 :golf:")
    st.caption("Follow the tournament, check the standings, and manage your pool entry.")

    rosters_count = session.sql(
        "SELECT COUNT(*) AS ENTRY_COUNT FROM PGA_MASTERS_2026.DATA.PLAYER_ROSTERS"
    ).to_pandas().iloc[0]["ENTRY_COUNT"]
    leaderboard_count = session.sql(
        "SELECT COUNT(*) AS PLAYER_COUNT FROM PGA_MASTERS_2026.DATA.LEADERBOARD"
    ).to_pandas().iloc[0]["PLAYER_COUNT"]

    metric_1, metric_2 = st.columns(2)
    metric_1.metric("Pool Entries", int(rosters_count))
    metric_2.metric("Players Tracked", int(leaderboard_count))

    st.divider()
    st.write("##### Where to go")
    st.write("Use the navigation menu to move between the main sections of the app.")
    st.write("`Leaderboard` shows team standings and the live tournament board.")
    st.write("`Teams` shows submitted entries and lets eligible users create a roster.")
