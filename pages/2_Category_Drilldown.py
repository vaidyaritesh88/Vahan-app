"""Page 2: Category Drilldown - OEM breakdown, volumes, market share, growth rates."""
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_oem_volumes_for_category, get_oem_share_trend,
    get_category_monthly_trend, get_available_months,
    get_oem_growth_rates, get_oem_quarterly_share,
    get_oem_annual_share, get_oem_fytd_share,
)
from components.filters import month_selector, category_selector, top_n_selector
from components.formatters import format_units, format_month, format_fy, get_fy_start_year, get_fy_label
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

# ── OEM Growth Rates (YoY / QoQ / MoM) ──
st.subheader("OEM Growth Rates")
growth_data = get_oem_growth_rates(cat_code, year, month, top_n)
if not growth_data.empty:
    display_growth = growth_data.copy()
    display_growth["Volume"] = display_growth["volume"].apply(lambda v: format_units(v))

    def _fmt_growth(val):
        if pd.isna(val):
            return "N/A"
        return f"{val:+.1f}%"

    display_growth["MoM %"] = display_growth["mom_pct"].apply(_fmt_growth)
    display_growth["QoQ %"] = display_growth["qoq_pct"].apply(_fmt_growth)
    display_growth["YoY %"] = display_growth["yoy_pct"].apply(_fmt_growth)
    display_growth["Share %"] = display_growth["share_pct"].apply(lambda v: f"{v:.1f}%")

    st.dataframe(
        display_growth[["oem_name", "Volume", "Share %", "MoM %", "QoQ %", "YoY %"]].rename(
            columns={"oem_name": "OEM"}
        ),
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# ── Charts Row ──
col1, col2 = st.columns(2)

with col1:
    display = oem_data.copy()
    display["label"] = display.apply(
        lambda r: f"{format_units(r['volume'])} ({r['share_pct']:.1f}%)", axis=1
    )
    fig = horizontal_bar(display, x="volume", y="oem_name",
                         title="OEM Volumes", text="label")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = donut_chart(oem_data, names="oem_name", values="volume",
                      title="Market Share")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Market Share by Time Period ──
st.subheader("Market Share by Time Period")
period_view = st.radio("View", ["Monthly", "Quarterly", "Annual (FY)", "FYTD"],
                       horizontal=True, key="share_period")

if period_view == "Monthly":
    share_trend = get_oem_share_trend(cat_code, top_n=min(top_n, 7), months=24)
    if not share_trend.empty:
        fig = line_chart(share_trend, x="date", y="share_pct", color="oem_name",
                         title="OEM Monthly Market Share Trend (%)", height=450)
        fig.update_yaxes(title="Market Share %")
        st.plotly_chart(fig, use_container_width=True)

elif period_view == "Quarterly":
    q_data = get_oem_quarterly_share(cat_code, top_n=min(top_n, 7))
    if not q_data.empty:
        fig = line_chart(q_data, x="quarter", y="share_pct", color="oem_name",
                         title="OEM Quarterly Market Share (%)", height=450, markers=True)
        fig.update_yaxes(title="Market Share %")
        fig.update_xaxes(title="Quarter")
        st.plotly_chart(fig, use_container_width=True)

        # Also show quarterly volumes stacked
        fig2 = stacked_bar_chart(q_data, x="quarter", y="volume", color="oem_name",
                                  title="Quarterly Volume Contribution")
        st.plotly_chart(fig2, use_container_width=True)

elif period_view == "Annual (FY)":
    fy_data = get_oem_annual_share(cat_code, top_n=min(top_n, 7))
    if not fy_data.empty:
        fig = line_chart(fy_data, x="fy_label", y="share_pct", color="oem_name",
                         title="OEM Annual Market Share (FY) (%)", height=450, markers=True)
        fig.update_yaxes(title="Market Share %")
        fig.update_xaxes(title="Fiscal Year")
        st.plotly_chart(fig, use_container_width=True)

        fig2 = stacked_bar_chart(fy_data, x="fy_label", y="volume", color="oem_name",
                                  title="Annual Volume Contribution (FY)")
        st.plotly_chart(fig2, use_container_width=True)

elif period_view == "FYTD":
    fytd_data = get_oem_fytd_share(cat_code, year, month, top_n=min(top_n, 7))
    if not fytd_data.empty:
        fy_label = fytd_data["fy_label"].iloc[0]
        prev_label = fytd_data["prev_fy_label"].iloc[0]

        display_fytd = fytd_data.copy()
        display_fytd[f"{fy_label} Vol"] = display_fytd["fytd_vol"].apply(format_units)
        display_fytd[f"{fy_label} Share"] = display_fytd["fytd_share"].apply(lambda v: f"{v:.1f}%")
        display_fytd[f"{prev_label} Vol"] = display_fytd["prev_fytd_vol"].apply(
            lambda v: format_units(v) if pd.notna(v) else "N/A"
        )
        display_fytd[f"{prev_label} Share"] = display_fytd["prev_fytd_share"].apply(
            lambda v: f"{v:.1f}%" if pd.notna(v) else "N/A"
        )
        display_fytd["YoY %"] = display_fytd["yoy_pct"].apply(
            lambda v: f"{v:+.1f}%" if pd.notna(v) else "N/A"
        )

        st.markdown(f"**FYTD through {format_month(year, month)}**")
        st.dataframe(
            display_fytd[["oem_name", f"{fy_label} Vol", f"{fy_label} Share",
                          f"{prev_label} Vol", f"{prev_label} Share", "YoY %"]].rename(
                columns={"oem_name": "OEM"}
            ),
            use_container_width=True,
            hide_index=True,
        )

st.divider()

# ── Monthly Volume Stacked ──
st.subheader("Monthly Volumes by OEM")
share_trend_vol = get_oem_share_trend(cat_code, top_n=min(top_n, 7), months=24)
if not share_trend_vol.empty:
    fig = stacked_bar_chart(share_trend_vol, x="date", y="volume", color="oem_name",
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
