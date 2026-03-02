"""Page 4: OEM 360 View - Deep-dive into any OEM across categories and states."""
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_oem_all_categories, get_oem_share_in_categories,
    get_oem_volume_trend, get_oem_state_distribution,
    get_oem_state_share_trend, has_state_data, get_states_with_data,
)
from components.filters import month_selector, oem_selector
from components.formatters import format_units, format_month
from components.charts import (
    horizontal_bar, donut_chart, line_chart, monthly_bar_chart,
)

init_db()

st.set_page_config(page_title="OEM 360", page_icon="🏢", layout="wide")
st.title("OEM 360 View")

# Sidebar
oem = oem_selector(key="oem360")
year, month = month_selector(key="oem360_period")

if not oem or year is None:
    st.warning("Select an OEM and time period.")
    st.stop()

st.sidebar.divider()

# ── Category Presence ──
cat_data = get_oem_all_categories(oem, year, month)
if cat_data.empty:
    st.info(f"No data for {oem} in {format_month(year, month)}.")
    st.stop()

total_vol = cat_data["volume"].sum()

st.subheader(f"{oem} - {format_month(year, month)}")

# KPIs
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Volume", format_units(total_vol))
with col2:
    st.metric("Categories Active", len(cat_data))
with col3:
    top_cat = cat_data.iloc[0]
    st.metric("Largest Category", f"{top_cat['category_name']}")

st.divider()

# ── Category Breakdown ──
col1, col2 = st.columns(2)

with col1:
    fig = horizontal_bar(cat_data, x="volume", y="category_name",
                         title=f"{oem} - Volume by Category")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = donut_chart(cat_data, names="category_name", values="volume",
                      title="Category Mix")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Market Share Trend Across Categories ──
st.subheader("Market Share Trend by Category")
share_data = get_oem_share_in_categories(oem, months=24)
if not share_data.empty:
    fig = line_chart(share_data, x="date", y="share_pct", color="category_name",
                     title=f"{oem} - Market Share Trend (%)", height=450)
    fig.update_yaxes(title="Market Share %")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Volume Trend ──
st.subheader("Volume Trend")
vol_trend = get_oem_volume_trend(oem, months=24)
if not vol_trend.empty:
    vol_trend["date"] = pd.to_datetime(vol_trend[["year", "month"]].assign(day=1))
    fig = monthly_bar_chart(vol_trend, x="date", y="volume",
                            title=f"{oem} - Monthly Total Volume")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── State Distribution (if state data available) ──
if has_state_data():
    st.subheader("State-Level Distribution")

    # Let user pick which category to see state distribution for
    cat_options = dict(zip(cat_data["category_name"], cat_data["category_code"]))
    selected_cat_name = st.selectbox("Category for state analysis", list(cat_options.keys()))
    selected_cat = cat_options[selected_cat_name]

    state_dist = get_oem_state_distribution(oem, selected_cat, year, month)
    if not state_dist.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = horizontal_bar(state_dist.head(15), x="volume", y="state",
                                 title=f"Top States - {oem} ({selected_cat_name})")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # State share trend - pick a state
            states = state_dist["state"].tolist()
            selected_state = st.selectbox("Track share in state", states[:10])
            if selected_state:
                share_trend = get_oem_state_share_trend(oem, selected_cat, selected_state, months=12)
                if not share_trend.empty:
                    fig = line_chart(share_trend, x="date", y="share_pct",
                                     title=f"Share Trend in {selected_state}")
                    fig.update_yaxes(title="Market Share %")
                    st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No state data for this OEM/category. Scrape Vahan to populate.")
else:
    st.info("State-level data not yet available. Use the Vahan scraper to populate state data.")
