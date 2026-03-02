"""Vahan Vehicle Registration Tracker - Main Entry Point."""
import streamlit as st
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.schema import init_db

st.set_page_config(
    page_title="Vahan Tracker",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize database on first run
if "db_initialized" not in st.session_state:
    init_db()
    st.session_state.db_initialized = True

st.sidebar.title("Vahan Tracker")
st.sidebar.markdown("Vehicle Registration Analytics")
st.sidebar.divider()

# Main landing page
st.title("India Vehicle Registration Tracker")
st.markdown("""
**Data Source:** Vahan Portal (Ministry of Road Transport & Highways)

Use the sidebar to navigate between analysis pages:

| Page | Description |
|------|-------------|
| **Category Overview** | National volumes & YoY growth across all vehicle categories |
| **Category Drilldown** | OEM breakdown, market share within a category |
| **Subsegment Mix** | ICE / EV / CNG / Hybrid powertrain analysis |
| **OEM 360** | Deep-dive into any OEM across categories and states |
| **State Performance** | Regional analysis & state-level OEM market share |
| **Data Management** | Upload Excel, run Vahan scraper, monitor data freshness |
""")

# Quick data status
from database.queries import get_record_counts, get_latest_month

counts = get_record_counts()
ly, lm = get_latest_month()

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("National Records", f"{counts['national_monthly']:,}")
with col2:
    st.metric("State Records", f"{counts['state_monthly']:,}")
with col3:
    from components.formatters import format_month
    st.metric("Latest Data", format_month(ly, lm))

if counts["national_monthly"] == 0:
    st.warning("No data loaded yet. Go to **Data Management** page to upload your Excel file.")
