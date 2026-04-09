"""Vahan Vehicle Registration Tracker - Main Entry Point.

Uses st.navigation() for grouped sidebar sections.
"""
import streamlit as st
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.schema import init_db

st.set_page_config(
    page_title="Vahan Tracker",
    page_icon="\U0001f697",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize database on first run
if "db_initialized" not in st.session_state:
    init_db()
    st.session_state.db_initialized = True

# ── Grouped Navigation ──
pages = {
    "Retail Sales (Sell-out)": [
        st.Page("pages/1_Category_Overview.py", title="Category Overview", icon=":material/bar_chart:"),
        st.Page("pages/2_Category_Drilldown.py", title="Category Drilldown", icon=":material/search:"),
        st.Page("pages/3_Subsegment_Mix.py", title="Subsegment Mix", icon=":material/bolt:"),
        st.Page("pages/4_OEM_360.py", title="OEM 360", icon=":material/apartment:"),
        st.Page("pages/5_State_Performance.py", title="State Performance", icon=":material/location_on:"),
    ],
    "Primary Sales (Sell-in)": [
        st.Page("pages/9_Primary_Sales.py", title="Category Overview", icon=":material/bar_chart:"),
        st.Page("pages/11_Primary_SubSegment.py", title="Sub-Segment Analysis", icon=":material/search:"),
        st.Page("pages/12_Primary_OEM_360.py", title="OEM 360", icon=":material/apartment:"),
    ],
    "Primary vs Retail": [
        st.Page("pages/10_Primary_vs_Retail.py", title="Growth Comparison", icon=":material/compare_arrows:"),
    ],
    "Tools": [
        st.Page("pages/6_Data_Management.py", title="Data Management", icon=":material/settings:"),
        st.Page("pages/8_AI_Chat.py", title="AI Chat", icon=":material/chat:"),
    ],
}

pg = st.navigation(pages)
pg.run()
