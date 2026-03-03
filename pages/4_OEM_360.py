"""Page 4: OEM 360 View - Deep-dive into any OEM with growth rates, market share, and powertrain analysis."""
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_oem_all_categories, get_oem_monthly_all, get_oem_with_market_totals,
    get_oem_categories_list, get_subsegments_for_base, get_category_monthly_all,
    get_oem_volume_trend, get_oem_state_distribution, get_oem_state_share_trend,
    has_state_data, get_latest_month, get_state_oem_monthly, get_state_category_monthly,
)
from components.filters import oem_selector, period_selector, frequency_selector
from components.formatters import format_units, format_month, format_pct
from components.charts import horizontal_bar, donut_chart, line_chart, monthly_bar_chart
from components.analysis import (
    compute_growth_rates, compute_fytd, compute_fy_volumes,
    aggregate_by_frequency, filter_by_period, get_period_months,
    add_fy_columns, compute_growth_series,
)

init_db()

st.set_page_config(page_title="OEM 360", page_icon="🏢", layout="wide")
st.title("OEM 360 View")


# ── Helper ──

def _safe_growth(current, previous):
    """Compute YoY/growth % safely."""
    if current is None or previous is None:
        return None
    if pd.isna(current) or pd.isna(previous) or previous <= 0:
        return None
    return round(((current / previous) - 1) * 100, 1)


# ── Sidebar Filters ──
oem = oem_selector(key="oem360")
preset, ref_year, ref_month = period_selector(key="oem360_period")

if not oem or ref_year is None:
    st.warning("Select an OEM and time period.")
    st.stop()

latest_year, latest_month = get_latest_month()
start_date, end_date = get_period_months(preset, ref_year, ref_month)

st.sidebar.divider()

# ── Load OEM data ──
oem_data = get_oem_monthly_all(oem)
if oem_data.empty:
    st.info(f"No data found for **{oem}**.")
    st.stop()

# Get base categories this OEM participates in
base_cats = get_oem_categories_list(oem)

st.subheader(f"{oem} — {format_month(ref_year, ref_month)}")

# ────────────────────────────────────────
# SECTION 1: SNAPSHOT KPIs (for reference month)
# ────────────────────────────────────────
cat_data = get_oem_all_categories(oem, ref_year, ref_month)
if cat_data.empty:
    st.warning(f"No data for {oem} in {format_month(ref_year, ref_month)}. Try a different reference month.")
    st.stop()

total_vol = cat_data["volume"].sum()

# Compute aggregate growth rates (total across all categories)
agg_monthly = oem_data.groupby(["year", "month", "date"])["volume"].sum().reset_index()
growth = compute_growth_rates(agg_monthly, ref_year, ref_month)
fytd = compute_fytd(agg_monthly, ref_year, ref_month)

col1, col2, col3, col4, col5, col6 = st.columns(6)
with col1:
    st.metric("Total Volume", format_units(total_vol))
with col2:
    st.metric("MoM Growth", format_pct(growth["mom_pct"]),
              help="Month-on-Month: reference month volume vs previous month volume")
with col3:
    st.metric(
        "QoQ Growth", format_pct(growth["qoq_pct"]),
        help="Compares the reference month's volume to the volume 3 months prior "
             "(e.g., Feb'26 vs Nov'25). This is a single-month comparison, "
             "not a sum of 3 months vs 3 months.",
    )
with col4:
    st.metric("YoY Growth", format_pct(growth["yoy_pct"]),
              help="Year-on-Year: reference month volume vs same month previous year")
with col5:
    st.metric("FYTD Volume", format_units(fytd["fytd_vol"]) if fytd["fytd_vol"] else "N/A",
              help="Fiscal Year To Date: cumulative volume from April of current FY through reference month")
with col6:
    st.metric("FYTD YoY", format_pct(fytd["fytd_yoy_pct"]),
              help="FYTD vs same period in previous fiscal year")

st.divider()

# ────────────────────────────────────────
# SECTION 2: MONTHLY SALES TABLE (Last 12 months)
# ────────────────────────────────────────
st.subheader("Monthly Sales")
st.caption("Last 12 months — absolute volumes, YoY growth, and rolling 12-month sales")

all_monthly = agg_monthly.sort_values("date").copy()
all_monthly["ym"] = all_monthly["year"].astype(int) * 100 + all_monthly["month"].astype(int)
vol_map = dict(zip(all_monthly["ym"], all_monthly["volume"]))

# YoY for each month
all_monthly["yoy_pct"] = all_monthly.apply(
    lambda r: _safe_growth(r["volume"], vol_map.get((int(r["year"]) - 1) * 100 + int(r["month"]))),
    axis=1,
)

# Rolling 12M
all_monthly["rolling_12m"] = all_monthly["volume"].rolling(12, min_periods=12).sum()

# Rolling 12M YoY
r12m_map = dict(zip(all_monthly["ym"], all_monthly["rolling_12m"]))
all_monthly["r12m_yoy_pct"] = all_monthly.apply(
    lambda r: _safe_growth(r["rolling_12m"], r12m_map.get((int(r["year"]) - 1) * 100 + int(r["month"]))),
    axis=1,
)

# Display last 12 months (latest first)
last_12 = all_monthly.tail(12).iloc[::-1]
display_monthly = pd.DataFrame({
    "Month": last_12.apply(lambda r: format_month(int(r["year"]), int(r["month"])), axis=1).values,
    "Volume": last_12["volume"].apply(format_units).values,
    "YoY %": last_12["yoy_pct"].apply(lambda x: format_pct(x) if pd.notna(x) else "—").values,
    "Rolling 12M": last_12["rolling_12m"].apply(lambda x: format_units(x) if pd.notna(x) else "—").values,
    "R12M YoY %": last_12["r12m_yoy_pct"].apply(lambda x: format_pct(x) if pd.notna(x) else "—").values,
})
st.dataframe(display_monthly, use_container_width=True, hide_index=True)

st.divider()

# ────────────────────────────────────────
# SECTION 3: QUARTERLY SALES TABLE
# ────────────────────────────────────────
st.subheader("Quarterly Sales")
st.caption("Financial quarters (1Q = Apr-Jun, 2Q = Jul-Sep, 3Q = Oct-Dec, 4Q = Jan-Mar)")

q_data = add_fy_columns(all_monthly.copy())
quarterly = q_data.groupby(["fy", "quarter", "q_label"]).agg(
    volume=("volume", "sum"),
    month_count=("month", "count"),
).reset_index()
quarterly = quarterly.sort_values(["fy", "quarter"])

# Build lookup for complete-quarter volumes
q_vol_lookup = {}
for _, r in quarterly.iterrows():
    q_vol_lookup[(int(r["fy"]), int(r["quarter"]))] = r["volume"]

# Track which months belong to each quarter (for partial-quarter fair comparison)
q_month_sets = q_data.groupby(["fy", "quarter"])["month"].apply(lambda s: set(s.astype(int))).to_dict()

# YoY for each quarter — partial quarters compare to same months from previous year
q_yoy_list = []
for _, r in quarterly.iterrows():
    fy, q, vol, mc = int(r["fy"]), int(r["quarter"]), r["volume"], int(r["month_count"])
    if mc < 3:
        # Partial quarter: compare to same months from prev year's same quarter
        current_months = q_month_sets.get((fy, q), set())
        prev_q_data = q_data[
            (q_data["fy"] == fy - 1) & (q_data["quarter"] == q) &
            (q_data["month"].astype(int).isin(current_months))
        ]
        prev_vol = prev_q_data["volume"].sum() if not prev_q_data.empty else 0
    else:
        prev_vol = q_vol_lookup.get((fy - 1, q), 0)
    q_yoy_list.append(_safe_growth(vol, prev_vol))
quarterly["yoy_pct"] = q_yoy_list

# Format quarter labels
quarterly["display_label"] = quarterly.apply(
    lambda r: f"{int(r['quarter'])}QFY{str(int(r['fy']) + 1)[-2:]}"
              + (f" ({int(r['month_count'])}M)" if r["month_count"] < 3 else ""),
    axis=1,
)

# Show last 8 quarters (latest first)
display_q = quarterly.tail(8).iloc[::-1]
display_quarterly = pd.DataFrame({
    "Quarter": display_q["display_label"].values,
    "Volume": display_q["volume"].apply(format_units).values,
    "YoY %": display_q["yoy_pct"].apply(lambda x: format_pct(x) if pd.notna(x) else "—").values,
})
st.dataframe(display_quarterly, use_container_width=True, hide_index=True)

st.divider()

# ────────────────────────────────────────
# SECTION 4: FISCAL YEAR PERFORMANCE
# ────────────────────────────────────────
st.subheader("Fiscal Year Performance")

fy_data = add_fy_columns(all_monthly.copy())
fy_month_counts = fy_data.groupby("fy")["month"].count().to_dict()
fy_vols = fy_data.groupby(["fy", "fy_label"])["volume"].sum().reset_index()
fy_vols = fy_vols.sort_values("fy")

if not fy_vols.empty:
    latest_fy = int(fy_vols["fy"].max())
    fy_vol_lookup = dict(zip(fy_vols["fy"].astype(int), fy_vols["volume"]))

    fy_rows = []
    for _, r in fy_vols.iterrows():
        fy_num = int(r["fy"])
        label = r["fy_label"]
        vol = r["volume"]
        n_months = fy_month_counts.get(fy_num, 0)

        if fy_num == latest_fy and n_months < 12:
            # Incomplete FY: use FYTD comparison (same months from previous FY)
            label = f"{label} (FYTD {n_months}M)"
            current_fy_rows = fy_data[fy_data["fy"] == fy_num]
            prev_vol = 0
            for _, mr in current_fy_rows.iterrows():
                prev_match = all_monthly[
                    (all_monthly["year"] == int(mr["year"]) - 1) &
                    (all_monthly["month"] == int(mr["month"]))
                ]
                if not prev_match.empty:
                    prev_vol += prev_match["volume"].iloc[0]
            yoy = _safe_growth(vol, prev_vol)
        else:
            # Complete FY: compare to previous FY
            prev_fy_vol = fy_vol_lookup.get(fy_num - 1)
            yoy = _safe_growth(vol, prev_fy_vol)

        fy_rows.append({
            "Fiscal Year": label,
            "Volume": format_units(vol),
            "YoY Growth %": format_pct(yoy) if yoy is not None else "—",
        })

    st.dataframe(pd.DataFrame(fy_rows), use_container_width=True, hide_index=True)

st.divider()

# ────────────────────────────────────────
# SECTION 5: CATEGORY BREAKDOWN
# ────────────────────────────────────────
st.subheader("Category Breakdown")
col1, col2 = st.columns(2)

with col1:
    fig = horizontal_bar(cat_data, x="volume", y="category_name",
                         title=f"{oem} — Volume by Category")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = donut_chart(cat_data, names="category_name", values="volume",
                      title="Category Mix")
    st.plotly_chart(fig, use_container_width=True)

# Per-category growth rates (with Overall row at the top)
st.markdown("**Category-wise Growth Rates**")
cat_growth_rows = []

# Overall row first
cat_growth_rows.append({
    "Category": "**Overall**",
    "Volume": format_units(total_vol),
    "MoM %": format_pct(growth["mom_pct"]),
    "QoQ %": format_pct(growth["qoq_pct"]),
    "YoY %": format_pct(growth["yoy_pct"]),
    "FYTD Vol": format_units(fytd["fytd_vol"]) if fytd["fytd_vol"] else "N/A",
    "FYTD YoY %": format_pct(fytd["fytd_yoy_pct"]),
})

# Per-category rows
for _, row in cat_data.iterrows():
    cc = row["category_code"]
    cat_monthly = oem_data[oem_data["category_code"] == cc].copy()
    if cat_monthly.empty:
        continue
    g = compute_growth_rates(cat_monthly, ref_year, ref_month)
    f = compute_fytd(cat_monthly, ref_year, ref_month)
    cat_growth_rows.append({
        "Category": row["category_name"],
        "Volume": format_units(row["volume"]),
        "MoM %": format_pct(g["mom_pct"]),
        "QoQ %": format_pct(g["qoq_pct"]),
        "YoY %": format_pct(g["yoy_pct"]),
        "FYTD Vol": format_units(f["fytd_vol"]) if f["fytd_vol"] else "N/A",
        "FYTD YoY %": format_pct(f["fytd_yoy_pct"]),
    })

if cat_growth_rows:
    st.dataframe(pd.DataFrame(cat_growth_rows), use_container_width=True, hide_index=True)

st.divider()

# ────────────────────────────────────────
# SECTION 6: MARKET SHARE TRENDS
# ────────────────────────────────────────
st.subheader("Market Share Analysis")

if base_cats.empty:
    st.info("No base category data for market share analysis.")
else:
    # Let user pick which category to analyze share for
    cat_options = dict(zip(base_cats["category_name"], base_cats["category_code"]))
    share_cat_name = st.selectbox("Select category for share analysis", list(cat_options.keys()), key="share_cat")
    share_cat_code = cat_options[share_cat_name]

    freq_label = frequency_selector(key="share_freq")
    freq = freq_label.lower()

    # Get OEM share data
    share_data = get_oem_with_market_totals(oem, share_cat_code)
    if not share_data.empty:
        share_data = filter_by_period(share_data, start_date, end_date)

        if freq != "monthly":
            share_data = aggregate_by_frequency(share_data, freq, vol_col="oem_volume")
        else:
            share_data["period_label"] = share_data["date"].dt.strftime("%b %Y")

        if not share_data.empty:
            fig = line_chart(share_data, x="date", y="share_pct",
                             title=f"{oem} — {share_cat_name} Market Share ({freq_label})", height=420)
            fig.update_yaxes(title="Market Share %")
            st.plotly_chart(fig, use_container_width=True)

            # ── Powertrain Breakdown (ICE vs EV vs Overall) ──
            subs = get_subsegments_for_base(share_cat_code)
            ev_subs = subs[subs["code"].str.startswith("EV_")]

            if not ev_subs.empty:
                st.markdown(f"**Powertrain Share Breakdown — {share_cat_name}**")
                st.caption(f"Overall {share_cat_name} share vs EV-only share vs ICE-only share")

                # Overall share already computed above
                overall_share = share_data[["date", "share_pct"]].copy()
                overall_share["type"] = f"Overall {share_cat_name}"

                # EV share: OEM's EV volume / total EV volume
                ev_code = ev_subs["code"].iloc[0]
                ev_share_data = get_oem_with_market_totals(oem, ev_code)
                ev_rows = pd.DataFrame()
                if not ev_share_data.empty:
                    ev_share_data = filter_by_period(ev_share_data, start_date, end_date)
                    if freq != "monthly":
                        ev_share_data = aggregate_by_frequency(ev_share_data, freq, vol_col="oem_volume")
                    if not ev_share_data.empty:
                        ev_rows = ev_share_data[["date", "share_pct"]].copy()
                        ev_rows["type"] = f"EV {share_cat_name}"

                # ICE share: computed from (overall vol - EV vol) / (total - total EV)
                base_total = get_category_monthly_all(share_cat_code)
                ev_total = get_category_monthly_all(ev_code)
                oem_base = get_oem_with_market_totals(oem, share_cat_code)
                oem_ev_raw = get_oem_with_market_totals(oem, ev_code)

                ice_rows = pd.DataFrame()
                if not oem_base.empty and not base_total.empty:
                    # Safely handle OEMs with no EV data (e.g., Royal Enfield)
                    if not oem_ev_raw.empty and "date" in oem_ev_raw.columns and "oem_volume" in oem_ev_raw.columns:
                        ev_oem_merge = oem_ev_raw[["date", "oem_volume"]].rename(columns={"oem_volume": "ev_oem_vol"})
                    else:
                        ev_oem_merge = pd.DataFrame(columns=["date", "ev_oem_vol"])

                    ice_calc = oem_base[["date", "oem_volume"]].merge(
                        ev_oem_merge, on="date", how="left"
                    )
                    ice_calc["ev_oem_vol"] = ice_calc["ev_oem_vol"].fillna(0)
                    ice_calc["ice_oem_vol"] = ice_calc["oem_volume"] - ice_calc["ev_oem_vol"]

                    cat_merge = base_total[["date", "volume"]].rename(columns={"volume": "base_vol"})
                    ev_merge = ev_total[["date", "volume"]].rename(columns={"volume": "ev_vol"}) if not ev_total.empty else pd.DataFrame(columns=["date", "ev_vol"])
                    ice_calc = ice_calc.merge(cat_merge, on="date", how="left")
                    ice_calc = ice_calc.merge(ev_merge, on="date", how="left")
                    ice_calc["ev_vol"] = ice_calc["ev_vol"].fillna(0)
                    ice_calc["ice_total"] = ice_calc["base_vol"] - ice_calc["ev_vol"]

                    ice_calc = filter_by_period(ice_calc, start_date, end_date)
                    mask = ice_calc["ice_total"] > 0
                    ice_calc.loc[mask, "share_pct"] = (ice_calc.loc[mask, "ice_oem_vol"] / ice_calc.loc[mask, "ice_total"] * 100).round(2)

                    if not ice_calc.empty:
                        ice_rows = ice_calc[["date", "share_pct"]].dropna(subset=["share_pct"]).copy()
                        ice_rows["type"] = f"ICE {share_cat_name}"

                # Combine all
                combined = pd.concat([overall_share, ev_rows, ice_rows], ignore_index=True)
                if not combined.empty and combined["type"].nunique() > 1:
                    fig = line_chart(combined, x="date", y="share_pct", color="type",
                                     title=f"{oem} — Share by Powertrain ({share_cat_name})", height=420)
                    fig.update_yaxes(title="Market Share %")
                    st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"No market share data for {oem} in {share_cat_name}.")

st.divider()

# ────────────────────────────────────────
# SECTION 7: VOLUME TREND WITH GROWTH RATES
# ────────────────────────────────────────
st.subheader("Volume Trend")

vol_trend = agg_monthly.copy()
vol_trend = filter_by_period(vol_trend, start_date, end_date)

if not vol_trend.empty:
    vol_trend = vol_trend.sort_values("date")
    fig = monthly_bar_chart(vol_trend, x="date", y="volume",
                            title=f"{oem} — Monthly Total Volume")
    st.plotly_chart(fig, use_container_width=True)

    # Growth rates table
    vol_with_growth = compute_growth_series(vol_trend)
    if not vol_with_growth.empty:
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
# SECTION 8: STATE-LEVEL ANALYSIS
# ────────────────────────────────────────
if has_state_data():
    st.subheader("State-Level Analysis")

    cat_options_state = dict(zip(cat_data["category_name"], cat_data["category_code"]))
    selected_cat_name = st.selectbox("Category for state analysis", list(cat_options_state.keys()), key="state_cat")
    selected_cat = cat_options_state[selected_cat_name]

    state_dist = get_oem_state_distribution(oem, selected_cat, ref_year, ref_month)
    if not state_dist.empty:
        col1, col2 = st.columns(2)
        with col1:
            fig = horizontal_bar(state_dist.head(15), x="volume", y="state",
                                 title=f"Top States — {oem} ({selected_cat_name})")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            states = state_dist["state"].tolist()
            selected_state = st.selectbox("Track share in state", states[:10], key="state_share")
            if selected_state:
                share_trend = get_oem_state_share_trend(oem, selected_cat, selected_state, months=60)
                if not share_trend.empty:
                    share_trend = filter_by_period(share_trend, start_date, end_date)
                    if not share_trend.empty:
                        fig = line_chart(share_trend, x="date", y="share_pct",
                                         title=f"Share Trend in {selected_state}")
                        fig.update_yaxes(title="Market Share %")
                        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No state data for this OEM/category. Scrape Vahan to populate.")
else:
    st.info("State-level data not yet available. Use the Vahan scraper to populate state data.")
