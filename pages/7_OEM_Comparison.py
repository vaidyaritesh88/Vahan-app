"""Page 7: OEM Comparison Dashboard - Compare top OEMs side-by-side on growth & share."""
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_oem_growth_rates, get_oem_quarterly_share, get_oem_annual_share,
    get_oem_fytd_share, get_multi_oem_monthly_trend,
    get_top_oems_for_category, get_all_oems_for_category,
    get_available_months,
)
from components.filters import month_selector, category_selector, top_n_selector
from components.formatters import (
    format_units, format_month, format_fy, get_fy_start_year, get_fy_label,
)
from components.charts import line_chart, stacked_bar_chart, monthly_bar_chart

init_db()

st.set_page_config(page_title="OEM Comparison", page_icon="📈", layout="wide")
st.title("OEM Comparison Dashboard")
st.caption("Compare growth rates and market share of top OEMs side-by-side")

# Sidebar
cat_code = category_selector(include_subsegments=True, key="cmp_cat",
                              label="Vehicle Category")
year, month = month_selector(key="cmp_period")

if not cat_code or year is None:
    st.warning("Select a category and time period.")
    st.stop()

# OEM multi-select: default to top 5
all_oems = get_all_oems_for_category(cat_code)
default_oems = get_top_oems_for_category(cat_code, year, month, top_n=5)

selected_oems = st.sidebar.multiselect(
    "Select OEMs to compare",
    all_oems,
    default=default_oems,
    key="cmp_oems",
)

if not selected_oems:
    st.info("Select at least one OEM from the sidebar.")
    st.stop()

st.sidebar.divider()
st.sidebar.markdown(f"**Comparing {len(selected_oems)} OEMs in {cat_code}**")

# ═══════════════════════════════════════════════
# SECTION 1: Growth Rate Comparison Table
# ═══════════════════════════════════════════════

st.subheader(f"Growth Rates - {format_month(year, month)}")

growth = get_oem_growth_rates(cat_code, year, month, top_n=50)
if not growth.empty:
    # Filter to selected OEMs
    cmp_growth = growth[growth["oem_name"].isin(selected_oems)].copy()

    if not cmp_growth.empty:
        def _fmt(val):
            if pd.isna(val):
                return "N/A"
            return f"{val:+.1f}%"

        display = cmp_growth.copy()
        display["Volume"] = display["volume"].apply(format_units)
        display["Share %"] = display["share_pct"].apply(lambda v: f"{v:.1f}%")
        display["MoM %"] = display["mom_pct"].apply(_fmt)
        display["QoQ %"] = display["qoq_pct"].apply(_fmt)
        display["YoY %"] = display["yoy_pct"].apply(_fmt)

        st.dataframe(
            display[["oem_name", "Volume", "Share %", "MoM %", "QoQ %", "YoY %"]].rename(
                columns={"oem_name": "OEM"}
            ),
            use_container_width=True, hide_index=True,
        )

        # Growth rate comparison bar charts
        import plotly.graph_objects as go
        from components.formatters import OEM_COLORS

        fig = go.Figure()
        for i, metric in enumerate([("yoy_pct", "YoY"), ("qoq_pct", "QoQ"), ("mom_pct", "MoM")]):
            col_name, label = metric
            vals = cmp_growth[col_name].tolist()
            fig.add_trace(go.Bar(
                name=label,
                x=cmp_growth["oem_name"],
                y=vals,
                text=[_fmt(v) for v in vals],
                textposition="outside",
                marker_color=OEM_COLORS[i],
            ))
        fig.update_layout(
            barmode="group", title="Growth Rate Comparison",
            height=400, template="plotly_white",
            margin=dict(l=40, r=20, t=40, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        fig.update_yaxes(title="Growth %")
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ═══════════════════════════════════════════════
# SECTION 2: Market Share Trends
# ═══════════════════════════════════════════════

st.subheader("Market Share Trends")
period_view = st.radio("Time Granularity", ["Monthly", "Quarterly", "Annual (FY)", "FYTD"],
                       horizontal=True, key="cmp_period_view")

if period_view == "Monthly":
    trend = get_multi_oem_monthly_trend(cat_code, selected_oems, months=24)
    if not trend.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = line_chart(trend, x="date", y="share_pct", color="oem_name",
                             title="Monthly Market Share (%)", height=420)
            fig.update_yaxes(title="Share %")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = line_chart(trend, x="date", y="volume", color="oem_name",
                             title="Monthly Volumes", height=420)
            fig.update_yaxes(title="Units")
            st.plotly_chart(fig, use_container_width=True)

elif period_view == "Quarterly":
    q_data = get_oem_quarterly_share(cat_code, top_n=50)
    if not q_data.empty:
        q_filtered = q_data[q_data["oem_name"].isin(selected_oems)]
        if not q_filtered.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = line_chart(q_filtered, x="quarter", y="share_pct", color="oem_name",
                                 title="Quarterly Market Share (%)", height=420, markers=True)
                fig.update_yaxes(title="Share %")
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = line_chart(q_filtered, x="quarter", y="volume", color="oem_name",
                                 title="Quarterly Volumes", height=420, markers=True)
                fig.update_yaxes(title="Units")
                st.plotly_chart(fig, use_container_width=True)

elif period_view == "Annual (FY)":
    fy_data = get_oem_annual_share(cat_code, top_n=50)
    if not fy_data.empty:
        fy_filtered = fy_data[fy_data["oem_name"].isin(selected_oems)]
        if not fy_filtered.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = line_chart(fy_filtered, x="fy_label", y="share_pct", color="oem_name",
                                 title="Annual Market Share (FY) (%)", height=420, markers=True)
                fig.update_yaxes(title="Share %")
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = line_chart(fy_filtered, x="fy_label", y="volume", color="oem_name",
                                 title="Annual Volumes (FY)", height=420, markers=True)
                fig.update_yaxes(title="Units")
                st.plotly_chart(fig, use_container_width=True)

elif period_view == "FYTD":
    fytd = get_oem_fytd_share(cat_code, year, month, top_n=50)
    if not fytd.empty:
        fytd_filtered = fytd[fytd["oem_name"].isin(selected_oems)]
        if not fytd_filtered.empty:
            fy_label = fytd_filtered["fy_label"].iloc[0]
            prev_label = fytd_filtered["prev_fy_label"].iloc[0]

            disp = fytd_filtered.copy()
            disp[f"{fy_label} Volume"] = disp["fytd_vol"].apply(format_units)
            disp[f"{fy_label} Share"] = disp["fytd_share"].apply(lambda v: f"{v:.1f}%")
            disp[f"{prev_label} Volume"] = disp["prev_fytd_vol"].apply(
                lambda v: format_units(v) if pd.notna(v) else "N/A"
            )
            disp[f"{prev_label} Share"] = disp["prev_fytd_share"].apply(
                lambda v: f"{v:.1f}%" if pd.notna(v) else "N/A"
            )
            disp["YoY Growth"] = disp["yoy_pct"].apply(
                lambda v: f"{v:+.1f}%" if pd.notna(v) else "N/A"
            )
            disp["Share Change"] = (disp["fytd_share"] - disp["prev_fytd_share"]).apply(
                lambda v: f"{v:+.1f}pp" if pd.notna(v) else "N/A"
            )

            st.markdown(f"**FYTD through {format_month(year, month)}**")
            st.dataframe(
                disp[["oem_name", f"{fy_label} Volume", f"{fy_label} Share",
                      f"{prev_label} Volume", f"{prev_label} Share",
                      "YoY Growth", "Share Change"]].rename(columns={"oem_name": "OEM"}),
                use_container_width=True, hide_index=True,
            )

st.divider()

# ═══════════════════════════════════════════════
# SECTION 3: Volume Indexed (Rebased to 100)
# ═══════════════════════════════════════════════

st.subheader("Volume Indexed Trend (Base = 100)")
st.caption("Shows relative growth trajectory - all OEMs rebased to 100 at the start of the period.")

trend_idx = get_multi_oem_monthly_trend(cat_code, selected_oems, months=24)
if not trend_idx.empty:
    # Rebase each OEM's volume to 100 at earliest month
    def rebase(group):
        group = group.sort_values("date")
        base_vol = group["volume"].iloc[0]
        group["indexed"] = (group["volume"] / base_vol * 100).round(1) if base_vol > 0 else 100
        return group

    trend_idx = trend_idx.groupby("oem_name", group_keys=False).apply(rebase)

    fig = line_chart(trend_idx, x="date", y="indexed", color="oem_name",
                     title="Volume Index (Base = 100 at Start)", height=420)
    fig.update_yaxes(title="Index (100 = Base)")
    fig.add_hline(y=100, line_dash="dash", line_color="gray", opacity=0.5)
    st.plotly_chart(fig, use_container_width=True)
