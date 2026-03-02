"""Page 3: Subsegment Mix - ICE / EV / CNG / Hybrid analysis."""
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_subsegment_mix, get_subsegment_trend,
    get_subsegment_oem_breakdown,
)
from components.filters import month_selector, base_category_selector, top_n_selector
from components.formatters import format_units, format_month
from components.charts import donut_chart, line_chart, horizontal_bar

init_db()

st.set_page_config(page_title="Subsegment Mix", page_icon="⚡", layout="wide")
st.title("Powertrain / Subsegment Mix")

# Sidebar
base_cat = base_category_selector()
year, month = month_selector(key="sub_period")

if not base_cat or year is None:
    st.warning("Select a base category and time period.")
    st.stop()

# ── Current Mix ──
mix = get_subsegment_mix(base_cat, year, month)
if mix.empty:
    st.info(f"No subsegment data for {base_cat} in {format_month(year, month)}.")
    st.stop()

base_total = mix["base_total"].iloc[0]
st.subheader(f"{base_cat} Subsegment Analysis - {format_month(year, month)}")

# KPI row
cols = st.columns(len(mix) + 1)
with cols[0]:
    st.metric(f"Total {base_cat}", format_units(base_total))

for i, (_, row) in enumerate(mix.iterrows()):
    with cols[i + 1]:
        st.metric(
            row["name"],
            format_units(row["volume"]),
            delta=f"{row['penetration_pct']:.1f}% penetration",
        )

st.divider()

# ── Penetration Trend ──
st.subheader("Penetration Trend")
trend = get_subsegment_trend(base_cat, months=36)
if not trend.empty:
    fig = line_chart(trend, x="date", y="penetration_pct", color="subsegment_name",
                     title=f"Subsegment Penetration in {base_cat} (%)", height=420)
    fig.update_yaxes(title="Penetration %")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Volume Trend ──
st.subheader("Volume Trend")
if not trend.empty:
    fig = line_chart(trend, x="date", y="volume", color="subsegment_name",
                     title=f"Subsegment Volumes", height=400)
    fig.update_yaxes(title="Units")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── OEM Breakdown per Subsegment ──
st.subheader("Top OEMs by Subsegment")
for _, row in mix.iterrows():
    sub_code = row["code"]
    sub_name = row["name"]
    oem_data = get_subsegment_oem_breakdown(sub_code, year, month, top_n=7)
    if not oem_data.empty:
        col1, col2 = st.columns([2, 1])
        with col1:
            fig = horizontal_bar(oem_data, x="volume", y="oem_name", title=f"{sub_name} - Top OEMs")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = donut_chart(oem_data, title=f"{sub_name} Share")
            st.plotly_chart(fig, use_container_width=True)
        st.divider()
