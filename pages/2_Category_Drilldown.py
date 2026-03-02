"""Page 2: Category Drilldown - OEM breakdown, volumes, market share."""
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_oem_volumes_for_category, get_oem_share_trend,
    get_category_monthly_trend, get_available_months,
)
from components.filters import month_selector, category_selector, top_n_selector
from components.formatters import format_units, format_month
from components.charts import (
    donut_chart, horizontal_bar, line_chart, stacked_bar_chart, monthly_bar_chart,
)

init_db()

st.set_page_config(page_title="Category Drilldown", page_icon="🔍", layout="wide")
st.title("Category Drilldown")

# Sidebar
cat_code = category_selector(include_subsegments=True)
year, month = month_selector()
top_n = top_n_selector()

if not cat_code or year is None:
    st.warning("Select a category and time period.")
    st.stop()

st.sidebar.divider()

# ── OEM Volumes ──
oem_data = get_oem_volumes_for_category(cat_code, year, month, top_n)
if oem_data.empty:
    st.info(f"No data for {cat_code} in {format_month(year, month)}.")
    st.stop()

total_vol = oem_data["volume"].sum()
st.subheader(f"{cat_code} - OEM Breakdown ({format_month(year, month)})")

# KPIs
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Volume", format_units(total_vol))
with col2:
    top_oem = oem_data.iloc[0]
    st.metric("Market Leader", f"{top_oem['oem_name']} ({top_oem['share_pct']:.1f}%)")
with col3:
    # HHI concentration
    hhi = ((oem_data["share_pct"] / 100) ** 2).sum()
    st.metric("HHI (Concentration)", f"{hhi:.3f}")

st.divider()

# ── Charts Row ──
col1, col2 = st.columns(2)

with col1:
    # Horizontal bar chart
    display = oem_data.copy()
    display["label"] = display.apply(
        lambda r: f"{format_units(r['volume'])} ({r['share_pct']:.1f}%)", axis=1
    )
    fig = horizontal_bar(display, x="volume", y="oem_name",
                         title="OEM Volumes", text="label")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    # Donut chart
    fig = donut_chart(oem_data, names="oem_name", values="volume",
                      title="Market Share")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Market Share Trend ──
st.subheader("Market Share Trend (Last 24 Months)")
share_trend = get_oem_share_trend(cat_code, top_n=min(top_n, 7), months=24)
if not share_trend.empty:
    fig = line_chart(share_trend, x="date", y="share_pct", color="oem_name",
                     title="OEM Market Share Trend (%)", height=450)
    fig.update_yaxes(title="Market Share %")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Monthly Volume Stacked ──
st.subheader("Monthly Volumes by OEM")
if not share_trend.empty:
    fig = stacked_bar_chart(share_trend, x="date", y="volume", color="oem_name",
                            title="Monthly Volume Contribution")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Data Table ──
st.subheader("Detailed OEM Data")
st.dataframe(
    oem_data[["oem_name", "volume", "share_pct"]].rename(columns={
        "oem_name": "OEM", "volume": "Volume", "share_pct": "Share %"
    }),
    use_container_width=True,
    hide_index=True,
)
