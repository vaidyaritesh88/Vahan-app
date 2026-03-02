"""Page 7: Industry Analysis - Category-level deep-dive with growth, EV penetration, and state analysis."""
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_category_monthly_all, get_latest_month, get_subsegments_for_base,
    get_ev_penetration_all, get_top_oems_for_period, get_oem_share_trend,
    get_state_volumes, has_state_data, get_state_category_monthly,
)
from components.filters import period_selector, frequency_selector, main_category_selector
from components.formatters import format_units, format_month, format_pct
from components.charts import (
    horizontal_bar, donut_chart, line_chart, monthly_bar_chart, yoy_bar_chart,
)
from components.analysis import (
    compute_growth_rates, compute_fytd, compute_fy_volumes,
    aggregate_by_frequency, filter_by_period, get_period_months,
    add_fy_columns, compute_growth_series,
)

init_db()

st.set_page_config(page_title="Industry Analysis", page_icon="📊", layout="wide")
st.title("Industry Analysis")

# ── Sidebar Filters ──
cat_code, cat_name = main_category_selector(key="ind_cat")
preset, ref_year, ref_month = period_selector(key="ind_period")

if not cat_code or ref_year is None:
    st.warning("Select a category and time period.")
    st.stop()

latest_year, latest_month = get_latest_month()
start_date, end_date = get_period_months(preset, ref_year, ref_month)

st.sidebar.divider()

# ── Load category data ──
cat_monthly = get_category_monthly_all(cat_code)
if cat_monthly.empty:
    st.info(f"No data found for **{cat_name}**.")
    st.stop()

st.subheader(f"{cat_name} — {format_month(ref_year, ref_month)}")

# ────────────────────────────────────────
# SECTION 1: INDUSTRY SNAPSHOT KPIs
# ────────────────────────────────────────
growth = compute_growth_rates(cat_monthly, ref_year, ref_month)
fytd = compute_fytd(cat_monthly, ref_year, ref_month)

# Current month volume
cur_vol = cat_monthly[(cat_monthly["year"] == ref_year) & (cat_monthly["month"] == ref_month)]
cur_volume = cur_vol["volume"].iloc[0] if not cur_vol.empty else 0

col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    st.metric(f"{cat_name} Volume", format_units(cur_volume))
with col2:
    st.metric("MoM Growth", format_pct(growth["mom_pct"]))
with col3:
    st.metric("QoQ Growth", format_pct(growth["qoq_pct"]))
with col4:
    st.metric("YoY Growth", format_pct(growth["yoy_pct"]))
with col5:
    st.metric("FYTD Volume", format_units(fytd["fytd_vol"]) if fytd["fytd_vol"] else "N/A")
with col6:
    st.metric("FYTD YoY", format_pct(fytd["fytd_yoy_pct"]))

st.divider()

# ────────────────────────────────────────
# SECTION 2: FY-OVER-FY PERFORMANCE
# ────────────────────────────────────────
st.subheader("Fiscal Year Performance")
fy_df = compute_fy_volumes(cat_monthly)
if not fy_df.empty:
    col1, col2 = st.columns(2)
    with col1:
        display_fy = fy_df[["fy_label", "volume", "yoy_pct"]].copy()
        display_fy.columns = ["Fiscal Year", "Volume", "YoY Growth %"]
        display_fy["Volume"] = display_fy["Volume"].apply(format_units)
        display_fy["YoY Growth %"] = display_fy["YoY Growth %"].apply(lambda x: format_pct(x) if pd.notna(x) else "—")
        st.dataframe(display_fy, use_container_width=True, hide_index=True)

    with col2:
        import plotly.graph_objects as go
        from components.charts import LAYOUT_DEFAULTS
        fig = go.Figure()
        colors = ["#2ca02c" if (v is not None and v >= 0) else "#d62728" for v in fy_df["yoy_pct"]]
        fig.add_trace(go.Bar(
            x=fy_df["fy_label"],
            y=fy_df["yoy_pct"],
            marker_color=colors,
            text=[format_pct(v) if pd.notna(v) else "—" for v in fy_df["yoy_pct"]],
            textposition="outside",
        ))
        fig.update_layout(**LAYOUT_DEFAULTS, height=350, title=f"{cat_name} — FY YoY Growth")
        fig.update_yaxes(title="YoY %")
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ────────────────────────────────────────
# SECTION 3: VOLUME TREND
# ────────────────────────────────────────
st.subheader("Volume Trend")
vol_filtered = filter_by_period(cat_monthly, start_date, end_date)

if not vol_filtered.empty:
    vol_filtered = vol_filtered.sort_values("date")
    fig = monthly_bar_chart(vol_filtered, x="date", y="volume",
                            title=f"{cat_name} — Monthly Volume")
    st.plotly_chart(fig, use_container_width=True)

    vol_with_growth = compute_growth_series(vol_filtered)
    with st.expander("Monthly Growth Rates Table"):
        display_df = vol_with_growth[["year", "month", "volume", "yoy_pct", "qoq_pct", "mom_pct"]].copy()
        display_df.insert(0, "Period", display_df.apply(lambda r: format_month(int(r["year"]), int(r["month"])), axis=1))
        display_df["volume"] = display_df["volume"].apply(format_units)
        for c in ["yoy_pct", "qoq_pct", "mom_pct"]:
            display_df[c] = display_df[c].apply(lambda x: format_pct(x) if pd.notna(x) else "—")
        display_df = display_df.rename(columns={"volume": "Volume", "yoy_pct": "YoY %", "qoq_pct": "QoQ %", "mom_pct": "MoM %"})
        display_df = display_df[["Period", "Volume", "YoY %", "QoQ %", "MoM %"]]
        st.dataframe(display_df, use_container_width=True, hide_index=True)

st.divider()

# ────────────────────────────────────────
# SECTION 4: EV PENETRATION ANALYSIS
# ────────────────────────────────────────
subs = get_subsegments_for_base(cat_code)
ev_subs = subs[subs["code"].str.startswith("EV_")] if not subs.empty else pd.DataFrame()

if not ev_subs.empty:
    st.subheader(f"EV Penetration — {cat_name}")

    ev_pen = get_ev_penetration_all(cat_code)
    if not ev_pen.empty:
        ev_pen = filter_by_period(ev_pen, start_date, end_date)

        if not ev_pen.empty:
            # Current EV metrics
            ev_cur = ev_pen[(ev_pen["year"] == ref_year) & (ev_pen["month"] == ref_month)]
            ev_growth = compute_growth_rates(ev_pen.rename(columns={"ev_volume": "volume"}), ref_year, ref_month)
            ev_fytd = compute_fytd(ev_pen.rename(columns={"ev_volume": "volume"}), ref_year, ref_month)

            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                pen_val = ev_cur["penetration_pct"].iloc[0] if not ev_cur.empty else None
                st.metric("EV Penetration", f"{pen_val:.1f}%" if pen_val else "N/A")
            with col2:
                ev_vol = ev_cur["ev_volume"].iloc[0] if not ev_cur.empty else 0
                st.metric("EV Volume", format_units(ev_vol))
            with col3:
                st.metric("EV YoY Growth", format_pct(ev_growth["yoy_pct"]))
            with col4:
                st.metric("EV MoM Growth", format_pct(ev_growth["mom_pct"]))
            with col5:
                st.metric("EV FYTD YoY", format_pct(ev_fytd["fytd_yoy_pct"]))

            # Penetration trend chart
            col1, col2 = st.columns(2)
            with col1:
                fig = line_chart(ev_pen, x="date", y="penetration_pct",
                                 title=f"EV Penetration of {cat_name} (%)", height=400)
                fig.update_yaxes(title="Penetration %")
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = monthly_bar_chart(ev_pen, x="date", y="ev_volume",
                                        title=f"EV {cat_name} — Monthly Volume")
                st.plotly_chart(fig, use_container_width=True)

            # EV FY performance
            ev_fy = compute_fy_volumes(ev_pen.rename(columns={"ev_volume": "volume"}))
            if not ev_fy.empty:
                with st.expander("EV Fiscal Year Performance"):
                    display_ev_fy = ev_fy[["fy_label", "volume", "yoy_pct"]].copy()
                    display_ev_fy.columns = ["Fiscal Year", "EV Volume", "YoY Growth %"]
                    display_ev_fy["EV Volume"] = display_ev_fy["EV Volume"].apply(format_units)
                    display_ev_fy["YoY Growth %"] = display_ev_fy["YoY Growth %"].apply(lambda x: format_pct(x) if pd.notna(x) else "—")
                    st.dataframe(display_ev_fy, use_container_width=True, hide_index=True)

    st.divider()

# ────────────────────────────────────────
# SECTION 5: TOP OEMs & MARKET SHARE
# ────────────────────────────────────────
st.subheader(f"Top OEMs — {cat_name}")

# Determine period boundaries for OEM ranking
sy, sm = start_date.year, start_date.month
ey, em = ref_year, ref_month

top_oems = get_top_oems_for_period(cat_code, sy, sm, ey, em, top_n=10)
if not top_oems.empty:
    col1, col2 = st.columns(2)
    with col1:
        fig = horizontal_bar(top_oems, x="volume", y="oem_name",
                             title=f"Top OEMs by Volume ({preset})")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = donut_chart(top_oems, names="oem_name", values="volume",
                          title="Market Share Distribution")
        st.plotly_chart(fig, use_container_width=True)

    # Market share trend for top OEMs
    share_trend = get_oem_share_trend(cat_code, top_n=7, months=120)
    if not share_trend.empty:
        share_trend = filter_by_period(share_trend, start_date, end_date)
        if not share_trend.empty:
            fig = line_chart(share_trend, x="date", y="share_pct", color="oem_name",
                             title=f"Market Share Trend — Top OEMs ({cat_name})", height=450)
            fig.update_yaxes(title="Market Share %")
            st.plotly_chart(fig, use_container_width=True)

st.divider()

# ────────────────────────────────────────
# SECTION 6: OTHER SUBSEGMENT ANALYSIS
# ────────────────────────────────────────
non_ev_subs = subs[~subs["code"].str.startswith("EV_")] if not subs.empty else pd.DataFrame()
if not non_ev_subs.empty:
    st.subheader(f"Other Subsegments — {cat_name}")
    for _, sub_row in non_ev_subs.iterrows():
        sub_code = sub_row["code"]
        sub_name = sub_row["name"]

        sub_monthly = get_category_monthly_all(sub_code)
        base_monthly = cat_monthly.copy()
        if sub_monthly.empty or base_monthly.empty:
            continue

        merged = sub_monthly[["date", "volume"]].rename(columns={"volume": "sub_volume"}).merge(
            base_monthly[["date", "volume"]].rename(columns={"volume": "base_volume"}),
            on="date", how="inner"
        )
        merged["penetration_pct"] = (merged["sub_volume"] / merged["base_volume"] * 100).round(2)
        merged = filter_by_period(merged, start_date, end_date)
        # Add year/month for growth calculation
        merged["year"] = merged["date"].dt.year
        merged["month"] = merged["date"].dt.month

        if not merged.empty:
            cur = merged[(merged["year"] == ref_year) & (merged["month"] == ref_month)]
            pen_val = cur["penetration_pct"].iloc[0] if not cur.empty else None
            sub_vol = cur["sub_volume"].iloc[0] if not cur.empty else 0

            col1, col2 = st.columns([1, 2])
            with col1:
                st.metric(f"{sub_name} Penetration", f"{pen_val:.1f}%" if pen_val else "N/A")
                st.metric(f"{sub_name} Volume", format_units(sub_vol))

            with col2:
                fig = line_chart(merged, x="date", y="penetration_pct",
                                 title=f"{sub_name} Penetration of {cat_name} (%)", height=350)
                fig.update_yaxes(title="Penetration %")
                st.plotly_chart(fig, use_container_width=True)

    st.divider()

# ────────────────────────────────────────
# SECTION 7: STATE-LEVEL ANALYSIS
# ────────────────────────────────────────
if has_state_data():
    st.subheader(f"State-Level Analysis — {cat_name}")

    state_dist = get_state_volumes(cat_code, ref_year, ref_month)
    if not state_dist.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = horizontal_bar(state_dist.head(15), x="volume", y="state",
                                 title=f"Top States — {cat_name}")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            total = state_dist["volume"].sum()
            state_dist_top = state_dist.head(10).copy()
            state_dist_top["share_pct"] = (state_dist_top["volume"] / total * 100).round(1)
            fig = donut_chart(state_dist_top, names="state", values="volume",
                              title="Top 10 States Share")
            st.plotly_chart(fig, use_container_width=True)

        # EV penetration by state (if EV subsegment exists)
        if not ev_subs.empty:
            st.markdown(f"**State-wise EV Penetration — {cat_name}**")
            ev_code = ev_subs["code"].iloc[0]
            ev_state = get_state_volumes(ev_code, ref_year, ref_month)
            if not ev_state.empty:
                ev_by_state = ev_state.rename(columns={"volume": "ev_volume"}).merge(
                    state_dist.rename(columns={"volume": "total_volume"}),
                    on="state", how="inner"
                )
                ev_by_state["ev_penetration_pct"] = (ev_by_state["ev_volume"] / ev_by_state["total_volume"] * 100).round(1)
                ev_by_state = ev_by_state.sort_values("ev_penetration_pct", ascending=False)

                col1, col2 = st.columns(2)
                with col1:
                    fig = horizontal_bar(ev_by_state.head(15), x="ev_penetration_pct", y="state",
                                         title=f"EV Penetration by State — {cat_name} (%)")
                    fig.update_xaxes(title="EV Penetration %")
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    fig = horizontal_bar(ev_by_state.head(15), x="ev_volume", y="state",
                                         title=f"EV Volume by State — {cat_name}")
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No state-level EV data available. Scrape Vahan to populate.")
    else:
        st.info("No state-level data for this period. Scrape Vahan to populate.")
else:
    st.info("State-level data not yet available. Use the Vahan scraper to populate state data.")
