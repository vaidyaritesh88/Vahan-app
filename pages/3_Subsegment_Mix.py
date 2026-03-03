"""Page 3: Subsegment Mix - ICE / EV / CNG / Hybrid analysis + state EV penetration."""
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_subsegment_mix, get_subsegment_trend,
    get_subsegment_oem_breakdown, get_state_ev_penetration,
    has_state_data,
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

# ── State-Level EV Penetration ──
if has_state_data() and base_cat in ("2W", "PV", "3W"):
    st.subheader(f"State-Level EV Penetration in {base_cat}")

    ev_pen = get_state_ev_penetration(base_cat, year, month)
    if not ev_pen.empty:
        col1, col2 = st.columns(2)

        with col1:
            # Top states by EV penetration
            top_ev = ev_pen.head(15).copy()
            top_ev["label"] = top_ev.apply(
                lambda r: f"{r['ev_penetration_pct']:.1f}% ({format_units(r['ev_volume'])} EV / {format_units(r['base_volume'])} total)",
                axis=1,
            )
            import plotly.express as px
            from components.formatters import OEM_COLORS
            fig = px.bar(
                top_ev, x="ev_penetration_pct", y="state", orientation="h",
                title=f"Top States by EV Penetration in {base_cat}",
                text="label", color_discrete_sequence=["#2ca02c"],
            )
            fig.update_layout(height=max(350, len(top_ev) * 35),
                              margin=dict(l=40, r=20, t=40, b=40))
            fig.update_yaxes(categoryorder="total ascending", title="")
            fig.update_xaxes(title="EV Penetration %")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Summary stats
            national_ev = ev_pen["ev_volume"].sum()
            national_base = ev_pen["base_volume"].sum()
            national_pen = (national_ev / national_base * 100) if national_base > 0 else 0

            st.metric("National EV Penetration", f"{national_pen:.1f}%")
            st.metric("States with >5% EV Penetration",
                      len(ev_pen[ev_pen["ev_penetration_pct"] > 5]))
            st.metric("Total EV Volume", format_units(national_ev))

            # Data table
            st.markdown("**All States:**")
            display_ev = ev_pen.copy()
            display_ev["EV Penetration"] = display_ev["ev_penetration_pct"].apply(lambda v: f"{v:.1f}%")
            display_ev["EV Volume"] = display_ev["ev_volume"].apply(format_units)
            display_ev["Total Volume"] = display_ev["base_volume"].apply(format_units)
            st.dataframe(
                display_ev[["state", "EV Volume", "Total Volume", "EV Penetration"]].rename(
                    columns={"state": "State"}
                ),
                use_container_width=True, hide_index=True, height=350,
            )
    else:
        st.info("No state-level data for EV penetration analysis. Scrape Vahan to populate.")

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
