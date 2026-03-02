"""Page 5: State Performance - Regional analysis & state-level OEM drilldown."""
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_state_volumes, get_state_oem_breakdown, has_state_data,
    get_states_with_data,
)
from components.filters import month_selector, category_selector, state_selector, top_n_selector
from components.formatters import format_units, format_month
from components.charts import horizontal_bar, donut_chart
from config.settings import STATE_TO_REGION

init_db()

st.set_page_config(page_title="State Performance", page_icon="📍", layout="wide")
st.title("State / Regional Performance")

if not has_state_data():
    st.warning("""
    **No state-level data available yet.**

    State data needs to be scraped from the Vahan portal.
    Go to the **Data Management** page to run the Vahan scraper.

    Once state data is populated, this page will show:
    - State-wise volume rankings
    - Regional heatmaps
    - State-level OEM market share
    - OEM share gain/loss by state
    """)
    st.stop()

# Sidebar
cat_code = category_selector(key="state_cat")
year, month = month_selector(key="state_period")
top_n = top_n_selector(key="state_top_n", default=15)

if not cat_code or year is None:
    st.stop()

# ── State Volumes ──
state_data = get_state_volumes(cat_code, year, month)
if state_data.empty:
    st.info(f"No state data for {cat_code} in {format_month(year, month)}.")
    st.stop()

total = state_data["volume"].sum()
state_data["share_pct"] = (state_data["volume"] / total * 100).round(1)

# Add region
state_data["region"] = state_data["state"].map(STATE_TO_REGION)

st.subheader(f"{cat_code} - State Rankings ({format_month(year, month)})")

# KPIs
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("National Total", format_units(total))
with col2:
    st.metric("States with Data", len(state_data))
with col3:
    top_state = state_data.iloc[0]
    st.metric("Top State", f"{top_state['state']} ({top_state['share_pct']:.1f}%)")

st.divider()

# ── Top States Bar Chart ──
col1, col2 = st.columns([3, 2])

with col1:
    display = state_data.head(top_n).copy()
    display["label"] = display.apply(
        lambda r: f"{format_units(r['volume'])} ({r['share_pct']:.1f}%)", axis=1
    )
    fig = horizontal_bar(display, x="volume", y="state",
                         title=f"Top {top_n} States", text="label")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Regional summary
    region_data = state_data.groupby("region", as_index=False).agg({"volume": "sum"})
    region_data = region_data.sort_values("volume", ascending=False)
    fig = donut_chart(region_data, names="region", values="volume",
                      title="Regional Distribution")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── State OEM Drilldown ──
st.subheader("State OEM Drilldown")
selected_state = st.selectbox("Select a state for OEM breakdown",
                               state_data["state"].tolist())

if selected_state:
    oem_data = get_state_oem_breakdown(cat_code, selected_state, year, month, top_n=10)
    if not oem_data.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = horizontal_bar(oem_data, x="volume", y="oem_name",
                                 title=f"{selected_state} - OEM Volumes")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = donut_chart(oem_data, names="oem_name", values="volume",
                              title=f"{selected_state} - Market Share")
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            oem_data[["oem_name", "volume", "share_pct"]].rename(columns={
                "oem_name": "OEM", "volume": "Volume", "share_pct": "Share %"
            }),
            use_container_width=True, hide_index=True,
        )
