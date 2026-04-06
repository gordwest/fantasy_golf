import streamlit as st

from home_page import render as render_home_page
from leaderboard_page import render as render_leaderboard_page
from teams_page import render as render_teams_page

st.set_page_config(page_title="Masters Pool - 2026", page_icon="⛳", layout="wide")

navigation = st.navigation(
    [
        st.Page(render_home_page, title="Home", icon=":material/home:", default=True, url_path="home"),
        st.Page(render_leaderboard_page, title="Leaderboard", icon=":material/leaderboard:", url_path="leaderboard"),
        st.Page(render_teams_page, title="Teams", icon=":material/groups:", url_path="teams"),
    ]
)

navigation.run()
