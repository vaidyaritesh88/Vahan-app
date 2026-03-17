"""Page 4: OEM 360 View — Deep-dive into any OEM.

Top section (national Excel data):
  1. KPI snapshot
  2. Total sales trend + YoY growth (dual-axis chart)
  3. Sub-category sales mix (100% stacked chart + volume table + YoY table)
  4. Market share trends (line chart per base category + sub-categories)

Bottom section (state scraped data):
  5. State sales split, contribution over time, YoY growth
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_oem_all_categories, get_oem_monthly_all, get_oem_with_market_totals,
    get_oem_categories_list, get_subsegments_for_base, get_category_monthly_all,
    get_oem_state_distribution, get_oem_state_share_trend, has_state_data,
    get_latest_month, get_state_oem_monthly, get_state_category_monthly,
    get_oem_state_market_shares,
    get_oem_vehcat_monthly, get_oem_fuel_monthly, get_last_scrape_info,
    get_oem_total_monthly_scraped, get_oem_category_breakdown_scraped,
    get_oem_category_monthly_scraped, get_market_category_monthly_scraped,
    get_oem_subsegment_monthly, get_subsegment_market_monthly,
    has_oem_subsegment_data,
)
from components.filters import oem_selector, period_selector
from components.formatters import (
    format_units, format_month, format_pct, OEM_COLORS,
    get_fy_start_year, get_fy_label,
)
from components.charts import (
    dual_axis_bar_line, line_chart, horizontal_bar, LAYOUT_DEFAULTS,
)
from config.oem_normalization import normalize_oem
from components.analysis import (
    compute_growth_rates, compute_fytd, compute_growth_series,
    add_fy_columns, filter_by_period, get_period_months, _pct_change,
    get_fy, aggregate_by_frequency,
)

init_db()

st.set_page_config(page_title="OEM 360", page_icon="🏢", layout="wide")
st.title("OEM 360 View")


# ──────────────────────────────────────
# HELPERS
# ──────────────────────────────────────
def _safe_growth(current, previous):
    if current is None or previous is None:
        return None
    if pd.isna(current) or pd.isna(previous) or previous <= 0:
        return None
    return round(((current / previous) - 1) * 100, 1)


def _fmt_vol(val):
    """Format volume as full number with commas for data tables."""
    if pd.isna(val) or val is None:
        return "—"
    return f"{int(val):,}"


def _fmt_growth(val):
    """Format growth % for table cells."""
    if val is None or pd.isna(val):
        return "—"
    return f"{val:+.1f}%"


def _period_lbl(row, freq):
    """Generate period label for a row based on frequency."""
    if freq != "monthly" and "period_label" in row.index:
        return row["period_label"]
    return format_month(int(row["year"]), int(row["month"]))


# CSS for colored growth values
_GROWTH_CSS = """
<style>
.growth-pos { color: #2ca02c; font-weight: 600; }
.growth-neg { color: #d62728; font-weight: 600; }
.growth-na { color: #999; }
.total-row { font-weight: 700; background-color: #f0f2f6; }
</style>
"""
st.markdown(_GROWTH_CSS, unsafe_allow_html=True)


# ──────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────
oem = oem_selector(key="oem360")
preset, ref_year, ref_month = period_selector(key="oem360_period")

if not oem or ref_year is None:
    st.warning("Select an OEM and time period.")
    st.stop()

FREQ_MAP = {"Monthly": "monthly", "Quarterly": "quarterly", "Financial Year": "annual"}
freq_label = st.sidebar.selectbox("Frequency", list(FREQ_MAP.keys()), key="oem360_freq")
freq = FREQ_MAP[freq_label]

st.sidebar.divider()

# Show last scrape date
_scrape_info = get_last_scrape_info()
if _scrape_info:
    with st.sidebar.expander("🔄 Last Scraped", expanded=False):
        for si in _scrape_info:
            stype = si['scrape_type'].replace('national_', 'N:').replace('state_', 'S:')
            ts = si['last_completed'][:16] if si['last_completed'] else 'Never'
            st.caption(f"**{stype}** — {ts}")

latest_year, latest_month = get_latest_month()
start_date, end_date = get_period_months(preset, ref_year, ref_month)

# ──────────────────────────────────────
# ──────────────────────────────────────
# LOAD DATA (from scraped national_oem_vehcat)
# ──────────────────────────────────────

# Normalize OEM name for scraped data
from config.oem_normalization import normalize_oem
oem_normalized = normalize_oem(oem) or oem

# Total monthly time series (for trend + KPI)
agg_monthly = get_oem_total_monthly_scraped(oem_normalized)
if agg_monthly.empty:
    st.info(f"No scraped data found for **{oem}** (normalized: {oem_normalized}).")
    st.stop()

agg_monthly["date"] = pd.to_datetime(
    agg_monthly["year"].astype(str) + "-" + agg_monthly["month"].astype(str).str.zfill(2) + "-01")
agg_monthly = agg_monthly.sort_values("date")

# Category breakdown for reference month
cat_breakdown = get_oem_category_breakdown_scraped(oem_normalized, ref_year, ref_month)
if cat_breakdown.empty:
    st.warning(f"No data for {oem_normalized} in {format_month(ref_year, ref_month)}. Try a different month.")
    st.stop()

total_vol = cat_breakdown["volume"].sum()

# Per-category monthly series (for Section 3 + 4)
cat_monthly = get_oem_category_monthly_scraped(oem_normalized)
cat_monthly["date"] = pd.to_datetime(
    cat_monthly["year"].astype(str) + "-" + cat_monthly["month"].astype(str).str.zfill(2) + "-01")

st.subheader(f"{oem}")
st.caption(f"Source: Vahan portal (scraped data) | Normalized as: {oem_normalized}")


# ════════════════════════════════════════
# SECTION 1: KPI SNAPSHOT
# ════════════════════════════════════════
growth = compute_growth_rates(agg_monthly, ref_year, ref_month)

# FYTD computation
from datetime import date as _dt_date
_today = _dt_date.today()
_today_fy = get_fy(_today.year, _today.month)
_data_fy  = get_fy(ref_year, ref_month)

if _today_fy == _data_fy:
    fytd = compute_fytd(agg_monthly, ref_year, ref_month)
else:
    fytd = {"fytd_vol": None, "prev_fytd_vol": None, "fytd_yoy_pct": None}

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(f"Volume ({format_month(ref_year, ref_month)})", format_units(total_vol))
with col2:
    st.metric(f"YoY ({format_month(ref_year, ref_month)})", format_pct(growth["yoy_pct"]))
with col3:
    fy_start = get_fy_start_year(ref_year, ref_month)
    st.metric(f"FYTD ({get_fy_label(fy_start)})",
              format_units(fytd["fytd_vol"]) if fytd["fytd_vol"] else "N/A")
with col4:
    st.metric("FYTD YoY",
              format_pct(fytd["fytd_yoy_pct"]) if fytd["fytd_yoy_pct"] is not None else "N/A")

st.divider()


# ════════════════════════════════════════
# SECTION 2: TOTAL SALES TREND + YoY
# ════════════════════════════════════════
st.subheader("Retail Sales Trend")

trend = agg_monthly.sort_values("date").copy()
trend = filter_by_period(trend, start_date, end_date)

if not trend.empty:
    if freq == "monthly":
        trend_agg = trend.copy()
        trend_agg["period_label"] = trend_agg.apply(
            lambda r: format_month(int(r["year"]), int(r["month"])), axis=1)
    else:
        trend_agg = add_fy_columns(trend)
        if freq == "quarterly":
            trend_agg = trend_agg.groupby(["fy", "quarter", "q_label"]).agg(
                volume=("volume", "sum")).reset_index()
            trend_agg["date"] = trend_agg.apply(
                lambda r: pd.Timestamp(int(r["fy"]) + (1 if r["quarter"] == 4 else 0),
                                       {1: 5, 2: 8, 3: 11, 4: 2}[int(r["quarter"])], 1), axis=1)
            trend_agg["period_label"] = trend_agg["q_label"]
        else:
            trend_agg = trend_agg.groupby(["fy", "fy_label"]).agg(
                volume=("volume", "sum")).reset_index()
            trend_agg["date"] = trend_agg["fy"].apply(lambda y: pd.Timestamp(int(y), 10, 1))
            trend_agg["period_label"] = trend_agg["fy_label"]

    trend_agg = trend_agg.sort_values("date")

    # Compute YoY for each period
    # Use full dataset (not period-filtered) for prior year lookups
    full_monthly = agg_monthly.copy()
    full_monthly = add_fy_columns(full_monthly)

    def _get_yoy_for_trend(row):
        if freq == "monthly":
            y, m = int(row["year"]), int(row["month"])
            prev = full_monthly[(full_monthly["year"] == y - 1) & (full_monthly["month"] == m)]
        elif freq == "quarterly":
            prev = full_monthly[full_monthly["fy"] == row["fy"] - 1]
            prev = prev[prev["quarter"] == row["quarter"]]
            prev = prev.groupby(["fy", "quarter"]).agg(volume=("volume", "sum")).reset_index()
        else:
            prev = full_monthly[full_monthly["fy"] == row["fy"] - 1]
            prev = prev.groupby("fy").agg(volume=("volume", "sum")).reset_index()
        if prev.empty:
            return None
        prev_vol = prev["volume"].sum()
        if prev_vol <= 0:
            return None
        return round(((row["volume"] / prev_vol) - 1) * 100, 1)

    trend_agg["yoy_pct"] = trend_agg.apply(_get_yoy_for_trend, axis=1)

    fig_trend = dual_axis_bar_line(
        trend_agg, x="period_label" if freq != "monthly" else "date",
        bar_y="volume", line_y="yoy_pct",
        title=f"{oem_normalized} — Retail Sales Trend",
        bar_name="Volume", line_name="YoY %",
    )
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("No trend data for the selected period.")

st.divider()


# ════════════════════════════════════════
# SECTION 3: CATEGORY MIX (from scraped VehCat data)
# ════════════════════════════════════════
st.subheader("Category Sales Mix")

if not cat_monthly.empty:
    cat_mix = cat_monthly.copy()
    cat_mix = filter_by_period(cat_mix, start_date, end_date)

    if not cat_mix.empty:
        # Aggregate by frequency
        if freq != "monthly":
            cat_mix = add_fy_columns(cat_mix)
            if freq == "quarterly":
                grp = ["base_category", "fy", "quarter", "q_label"]
            else:
                grp = ["base_category", "fy", "fy_label"]
            cat_mix = cat_mix.groupby(grp).agg(volume=("volume", "sum")).reset_index()
            if freq == "quarterly":
                cat_mix["period_label"] = cat_mix["q_label"]
                cat_mix["date"] = cat_mix.apply(
                    lambda r: pd.Timestamp(int(r["fy"]) + (1 if r["quarter"] == 4 else 0),
                                           {1: 5, 2: 8, 3: 11, 4: 2}[int(r["quarter"])], 1), axis=1)
            else:
                cat_mix["period_label"] = cat_mix["fy_label"]
                cat_mix["date"] = cat_mix["fy"].apply(lambda y: pd.Timestamp(int(y), 10, 1))
        else:
            cat_mix["period_label"] = cat_mix.apply(
                lambda r: format_month(int(r["year"]), int(r["month"])), axis=1)

        cat_mix = cat_mix.sort_values("date")
        period_labels = cat_mix.sort_values("date")["period_label"].unique().tolist()

        # Only show categories with meaningful volume
        cat_totals = cat_mix.groupby("base_category")["volume"].sum()
        sig_cats = cat_totals[cat_totals > 100].sort_values(ascending=False).index.tolist()

        if sig_cats:
            # Compute share % within each period
            period_total = cat_mix.groupby("period_label")["volume"].sum().to_dict()
            cat_mix["pct"] = cat_mix.apply(
                lambda r: round(r["volume"] / period_total[r["period_label"]] * 100, 1)
                if period_total.get(r["period_label"], 0) > 0 else 0, axis=1)

            # 3A: 100% Stacked Bar Chart
            pivot_pct = cat_mix.pivot_table(
                index="period_label", columns="base_category", values="pct", fill_value=0)
            pivot_pct = pivot_pct.reindex(period_labels)
            pivot_pct = pivot_pct.reindex(columns=[c for c in sig_cats if c in pivot_pct.columns])

            cat_colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3"]
            fig_mix = go.Figure()
            for i, col_name in enumerate(pivot_pct.columns):
                fig_mix.add_trace(go.Bar(
                    x=pivot_pct.index, y=pivot_pct[col_name],
                    name=col_name, marker_color=cat_colors[i % len(cat_colors)],
                ))
            freq_title = {"monthly": "Monthly", "quarterly": "Quarterly", "annual": "Annual"}[freq]
            fig_mix.update_layout(
                **LAYOUT_DEFAULTS,
                barmode="stack",
                title=f"{oem_normalized} — Category Mix ({freq_title}) (%)",
                yaxis_title="Share %",
                height=400,
            )
            fig_mix.update_xaxes(title="")
            st.plotly_chart(fig_mix, use_container_width=True)

            # 3B: Volume Table
            pivot_vol = cat_mix.pivot_table(
                index="base_category", columns="period_label", values="volume", fill_value=0)
            pivot_vol = pivot_vol.reindex(columns=period_labels)
            pivot_vol = pivot_vol.reindex([c for c in sig_cats if c in pivot_vol.index])

            total_row = pivot_vol.sum(axis=0)
            total_row.name = "TOTAL"
            pivot_vol = pd.concat([pivot_vol, total_row.to_frame().T])

            display_vol = pivot_vol.map(
                lambda x: _fmt_vol(x) if isinstance(x, (int, float, np.integer, np.floating)) else x)
            display_vol.index.name = "Category"

            st.markdown(f"**Category Volume ({freq_title})**")
            st.dataframe(display_vol, use_container_width=True)

            # 3C: YoY Growth Table
            # Use full data for prior-year lookup
            cat_full = get_oem_category_monthly_scraped(oem_normalized)
            cat_full["date"] = pd.to_datetime(
                cat_full["year"].astype(str) + "-" + cat_full["month"].astype(str).str.zfill(2) + "-01")
            if freq != "monthly":
                cat_full = add_fy_columns(cat_full)
                if freq == "quarterly":
                    cat_full = cat_full.groupby(["base_category", "fy", "quarter"]).agg(
                        volume=("volume", "sum")).reset_index()
                else:
                    cat_full = cat_full.groupby(["base_category", "fy"]).agg(
                        volume=("volume", "sum")).reset_index()

            vol_lookup = {}
            for _, r in cat_full.iterrows():
                bc = r["base_category"]
                if freq == "monthly":
                    pk = (int(r["year"]), int(r["month"]))
                elif freq == "quarterly":
                    pk = (int(r["fy"]), int(r["quarter"]))
                else:
                    pk = int(r["fy"])
                vol_lookup[(bc, pk)] = vol_lookup.get((bc, pk), 0) + r["volume"]

            growth_data = {}
            for bc in sig_cats + ["TOTAL"]:
                growth_data[bc] = {}

            for plbl in period_labels:
                rows_p = cat_mix[cat_mix["period_label"] == plbl]
                if rows_p.empty:
                    continue
                sample = rows_p.iloc[0]

                for bc in sig_cats:
                    if freq == "monthly":
                        pk = (int(sample["year"]), int(sample["month"]))
                        prev_pk = (int(sample["year"]) - 1, int(sample["month"]))
                    elif freq == "quarterly":
                        pk = (int(sample["fy"]), int(sample["quarter"]))
                        prev_pk = (int(sample["fy"]) - 1, int(sample["quarter"]))
                    else:
                        pk = int(sample["fy"])
                        prev_pk = int(sample["fy"]) - 1
                    curr = vol_lookup.get((bc, pk), 0)
                    prev = vol_lookup.get((bc, prev_pk), 0)
                    growth_data[bc][plbl] = _safe_growth(curr, prev)

                # TOTAL
                if freq == "monthly":
                    pk = (int(sample["year"]), int(sample["month"]))
                    prev_pk = (int(sample["year"]) - 1, int(sample["month"]))
                elif freq == "quarterly":
                    pk = (int(sample["fy"]), int(sample["quarter"]))
                    prev_pk = (int(sample["fy"]) - 1, int(sample["quarter"]))
                else:
                    pk = int(sample["fy"])
                    prev_pk = int(sample["fy"]) - 1
                curr_t = sum(v for (c, k), v in vol_lookup.items() if k == pk)
                prev_t = sum(v for (c, k), v in vol_lookup.items() if k == prev_pk)
                growth_data["TOTAL"][plbl] = _safe_growth(curr_t, prev_t)

            growth_df = pd.DataFrame(growth_data).T.reindex(columns=period_labels)
            growth_df.index.name = "Category"
            display_growth = growth_df.map(_fmt_growth)

            with st.expander(f"Category YoY Growth ({freq_title}) (%)", expanded=False):
                st.dataframe(display_growth, use_container_width=True)

    st.divider()


# ════════════════════════════════════════
# SECTION 3B: SUB-CATEGORY SALES MIX (from Selenium-scraped subsegment data)
# ════════════════════════════════════════
# This section shows EV/CNG/Hybrid/ICE breakdown within each base category
# Data comes from national_oem_subsegment table (Selenium scraper with checkbox filters)

if has_oem_subsegment_data(oem_normalized):
    # Get base categories this OEM participates in
    base_cats_df = get_oem_categories_list(oem_normalized)
    if not base_cats_df.empty:
        base_cats_with_subs = []
        for _, row in base_cats_df.iterrows():
            bc = row["category_code"]
            subs = get_subsegments_for_base(bc)
            if not subs.empty:
                base_cats_with_subs.append((bc, row["category_name"], subs))

        if base_cats_with_subs:
            st.subheader("Sub-Category Sales Mix")
            st.caption(
                "Breakdown of each vehicle category into sub-types "
                "(ICE, EV, CNG, Hybrid). Source: Vahan portal (Selenium scraper)"
            )

            for bc_code, bc_name, subs_df in base_cats_with_subs:
                st.markdown(f"#### {bc_name}")

                sub_codes = subs_df["code"].tolist()
                sub_names = dict(zip(subs_df["code"], subs_df["name"]))

                # Get OEM's subsegment monthly data
                sub_monthly = get_oem_subsegment_monthly(oem_normalized, bc_code)

                # Get base category monthly totals for this OEM
                bc_monthly = cat_monthly[cat_monthly["base_category"] == bc_code].copy()

                if sub_monthly.empty or bc_monthly.empty:
                    st.info(f"No sub-category data for {bc_name}.")
                    continue

                # Build combined DataFrame: one row per (year, month, sub_type)
                # including ICE = base_total - sum(subsegments)
                months_available = bc_monthly[["year", "month", "date", "volume"]].copy()
                months_available = months_available.rename(columns={"volume": "base_vol"})

                # Pivot subsegments: year, month -> sub_code -> volume
                sub_pivot = sub_monthly.pivot_table(
                    index=["year", "month"], columns="subsegment_code",
                    values="volume", fill_value=0
                ).reset_index()

                # Merge base volumes with subsegment data
                combined = months_available.merge(sub_pivot, on=["year", "month"], how="left")
                for sc in sub_codes:
                    if sc not in combined.columns:
                        combined[sc] = 0
                    combined[sc] = combined[sc].fillna(0)

                # Compute ICE = base - sum(subsegments), clipped to 0
                combined["ICE"] = (combined["base_vol"] - combined[sub_codes].sum(axis=1)).clip(lower=0)

                # Build long-form data: (year, month, date, sub_type, volume)
                sub_type_cols = ["ICE"] + sub_codes
                sub_type_names = {"ICE": f"ICE {bc_name}"}
                sub_type_names.update(sub_names)

                rows_long = []
                for _, r in combined.iterrows():
                    for st_code in sub_type_cols:
                        rows_long.append({
                            "year": int(r["year"]),
                            "month": int(r["month"]),
                            "date": r["date"],
                            "sub_type": st_code,
                            "sub_name": sub_type_names.get(st_code, st_code),
                            "volume": r[st_code],
                        })

                sub_long = pd.DataFrame(rows_long)
                sub_long = filter_by_period(sub_long, start_date, end_date)

                if sub_long.empty:
                    st.info(f"No sub-category data for {bc_name} in selected period.")
                    continue

                # Add period labels based on frequency
                if freq != "monthly":
                    sub_long = add_fy_columns(sub_long)
                    if freq == "quarterly":
                        sub_long = sub_long.groupby(["sub_type", "sub_name", "fy", "quarter", "q_label"]).agg(
                            volume=("volume", "sum")).reset_index()
                        sub_long["period_label"] = sub_long["q_label"]
                        sub_long["date"] = sub_long.apply(
                            lambda r: pd.Timestamp(int(r["fy"]) + (1 if r["quarter"] == 4 else 0),
                                                   {1: 5, 2: 8, 3: 11, 4: 2}[int(r["quarter"])], 1), axis=1)
                    else:
                        sub_long = sub_long.groupby(["sub_type", "sub_name", "fy", "fy_label"]).agg(
                            volume=("volume", "sum")).reset_index()
                        sub_long["period_label"] = sub_long["fy_label"]
                        sub_long["date"] = sub_long["fy"].apply(lambda y: pd.Timestamp(int(y), 10, 1))
                else:
                    sub_long["period_label"] = sub_long.apply(
                        lambda r: format_month(int(r["year"]), int(r["month"])), axis=1)

                sub_long = sub_long.sort_values("date")
                period_labels_sub = sub_long.sort_values("date")["period_label"].unique().tolist()

                # Compute share %
                period_totals_sub = sub_long.groupby("period_label")["volume"].sum().to_dict()
                sub_long["pct"] = sub_long.apply(
                    lambda r: round(r["volume"] / period_totals_sub[r["period_label"]] * 100, 1)
                    if period_totals_sub.get(r["period_label"], 0) > 0 else 0, axis=1)

                # Sub-type order: ICE first (usually largest), then subsegments
                type_order = [t for t in sub_type_cols if t in sub_long["sub_type"].unique()]

                # ── 3B-1: 100% Stacked Bar Chart ──
                pivot_sub_pct = sub_long.pivot_table(
                    index="period_label", columns="sub_type", values="pct", fill_value=0)
                pivot_sub_pct = pivot_sub_pct.reindex(period_labels_sub)
                pivot_sub_pct = pivot_sub_pct.reindex(columns=[c for c in type_order if c in pivot_sub_pct.columns])

                # Map sub_type codes to display names for chart
                display_names = {t: sub_type_names.get(t, t) for t in type_order}

                sub_colors = {
                    "ICE": "#636EFA",
                    "EV_PV": "#00CC96", "EV_2W": "#00CC96", "EV_3W": "#00CC96",
                    "PV_CNG": "#FFA15A",
                    "PV_HYBRID": "#AB63FA",
                }
                default_colors = ["#19D3F3", "#FF6692", "#B6E880", "#FF97FF"]

                fig_sub = go.Figure()
                for i, col_name in enumerate(pivot_sub_pct.columns):
                    color = sub_colors.get(col_name, default_colors[i % len(default_colors)])
                    fig_sub.add_trace(go.Bar(
                        x=pivot_sub_pct.index, y=pivot_sub_pct[col_name],
                        name=display_names.get(col_name, col_name),
                        marker_color=color,
                    ))

                freq_title_sub = {"monthly": "Monthly", "quarterly": "Quarterly", "annual": "Annual"}[freq]
                fig_sub.update_layout(
                    **LAYOUT_DEFAULTS,
                    barmode="stack",
                    title=f"{oem_normalized} — {bc_name} Sub-Category Mix ({freq_title_sub}) (%)",
                    yaxis_title="Share %",
                    height=400,
                )
                fig_sub.update_xaxes(title="")
                st.plotly_chart(fig_sub, use_container_width=True)

                # ── 3B-2: Volume Table ──
                pivot_sub_vol = sub_long.pivot_table(
                    index="sub_type", columns="period_label", values="volume", fill_value=0)
                pivot_sub_vol = pivot_sub_vol.reindex(columns=period_labels_sub)
                pivot_sub_vol = pivot_sub_vol.reindex([t for t in type_order if t in pivot_sub_vol.index])

                # Rename index to display names
                pivot_sub_vol.index = [display_names.get(t, t) for t in pivot_sub_vol.index]

                total_sub = pivot_sub_vol.sum(axis=0)
                total_sub.name = f"TOTAL ({bc_name})"
                pivot_sub_vol = pd.concat([pivot_sub_vol, total_sub.to_frame().T])

                display_sub_vol = pivot_sub_vol.map(
                    lambda x: _fmt_vol(x) if isinstance(x, (int, float, np.integer, np.floating)) else x)
                display_sub_vol.index.name = "Sub-Category"

                st.markdown(f"**{bc_name} Sub-Category Volume ({freq_title_sub})**")
                st.dataframe(display_sub_vol, use_container_width=True)

                # ── 3B-3: YoY Growth Table ──
                # Use full (unfiltered) data for prior-year comparisons
                sub_full = get_oem_subsegment_monthly(oem_normalized, bc_code)
                bc_full = get_oem_category_monthly_scraped(oem_normalized)
                bc_full = bc_full[bc_full["base_category"] == bc_code].copy()

                if not sub_full.empty and not bc_full.empty:
                    # Build per-month base totals
                    bc_lookup = {}
                    for _, r in bc_full.iterrows():
                        bc_lookup[(int(r["year"]), int(r["month"]))] = r["volume"]

                    # Build per-month per-subtype volumes
                    sub_vol_lookup = {}
                    for _, r in sub_full.iterrows():
                        sub_vol_lookup[(r["subsegment_code"], int(r["year"]), int(r["month"]))] = r["volume"]

                    # Compute ICE per month
                    ice_lookup = {}
                    for (y, m), base_v in bc_lookup.items():
                        sub_sum = sum(sub_vol_lookup.get((sc, y, m), 0) for sc in sub_codes)
                        ice_lookup[(y, m)] = max(0, base_v - sub_sum)

                    # Build growth for each period_label
                    growth_sub = {}
                    for st_code in type_order:
                        dn = display_names.get(st_code, st_code)
                        growth_sub[dn] = {}

                    growth_sub[f"TOTAL ({bc_name})"] = {}

                    for plbl in period_labels_sub:
                        rows_p = sub_long[sub_long["period_label"] == plbl]
                        if rows_p.empty:
                            continue
                        sample = rows_p.iloc[0]

                        if freq == "monthly":
                            y, m = int(sample["year"]), int(sample["month"])
                            py, pm = y - 1, m
                        elif freq == "quarterly":
                            # For quarterly/annual, we'd need more complex aggregation
                            # Skip detailed YoY for non-monthly for now
                            continue
                        else:
                            continue

                        total_curr, total_prev = 0, 0
                        for st_code in type_order:
                            dn = display_names.get(st_code, st_code)
                            if st_code == "ICE":
                                curr = ice_lookup.get((y, m), 0)
                                prev = ice_lookup.get((py, pm), 0)
                            else:
                                curr = sub_vol_lookup.get((st_code, y, m), 0)
                                prev = sub_vol_lookup.get((st_code, py, pm), 0)
                            growth_sub[dn][plbl] = _safe_growth(curr, prev)
                            total_curr += curr
                            total_prev += prev

                        growth_sub[f"TOTAL ({bc_name})"][plbl] = _safe_growth(total_curr, total_prev)

                    growth_sub_df = pd.DataFrame(growth_sub).T.reindex(columns=period_labels_sub)
                    growth_sub_df.index.name = "Sub-Category"
                    display_sub_growth = growth_sub_df.map(_fmt_growth)

                    if not growth_sub_df.dropna(how="all", axis=1).empty:
                        with st.expander(f"{bc_name} Sub-Category — YoY Growth ({freq_title_sub}) (%)", expanded=False):
                            st.dataframe(display_sub_growth, use_container_width=True)

                st.markdown("---")

            st.divider()


# ════════════════════════════════════════
# SECTION 4: MARKET SHARE TRENDS
# ════════════════════════════════════════
st.subheader("Market Share Trends")

# Load market totals (all OEMs combined)
market_monthly = get_market_category_monthly_scraped()
if not market_monthly.empty and not cat_monthly.empty:
    market_monthly["date"] = pd.to_datetime(
        market_monthly["year"].astype(str) + "-" + market_monthly["month"].astype(str).str.zfill(2) + "-01")

    # Categories where OEM has presence
    oem_cats = cat_monthly.groupby("base_category")["volume"].sum()
    active_cats = oem_cats[oem_cats > 100].sort_values(ascending=False).index.tolist()

    if active_cats:
        # Build share data
        share_rows = []
        for bc in active_cats:
            oem_bc = cat_monthly[cat_monthly["base_category"] == bc][["year", "month", "date", "volume"]].copy()
            mkt_bc = market_monthly[market_monthly["base_category"] == bc][["year", "month", "volume"]].copy()
            mkt_bc = mkt_bc.rename(columns={"volume": "market_vol"})

            merged = oem_bc.merge(mkt_bc, on=["year", "month"], how="inner")
            merged["share_pct"] = (merged["volume"] / merged["market_vol"] * 100).round(2)
            merged["base_category"] = bc
            share_rows.append(merged)

        if share_rows:
            share_df = pd.concat(share_rows, ignore_index=True)
            share_df = filter_by_period(share_df, start_date, end_date)
            share_df = share_df.sort_values("date")

            if not share_df.empty:
                fig_share = go.Figure()
                share_colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A"]
                for i, bc in enumerate(active_cats):
                    bc_data = share_df[share_df["base_category"] == bc].sort_values("date")
                    if not bc_data.empty:
                        fig_share.add_trace(go.Scatter(
                            x=bc_data["date"], y=bc_data["share_pct"],
                            mode="lines+markers", name=bc,
                            line=dict(width=2.5, color=share_colors[i % len(share_colors)]),
                            marker=dict(size=5),
                        ))

                fig_share.update_layout(
                    **LAYOUT_DEFAULTS,
                    title=f"{oem_normalized} — Market Share by Category (%)",
                    yaxis_title="Market Share %",
                    height=420,
                    hovermode="x unified",
                )
                fig_share.update_xaxes(title="")
                st.plotly_chart(fig_share, use_container_width=True)

    st.divider()


# ════════════════════════════════════════
# SECTION 5: VEHICLE CATEGORY BREAKDOWN (national scraped data)
# ════════════════════════════════════════
vehcat_data = get_oem_vehcat_monthly(oem_normalized)

if not vehcat_data.empty:
    # Check if monthly data (month > 0) exists
    vehcat_monthly = vehcat_data[vehcat_data["month"] > 0].copy()
    vehcat_annual = vehcat_data[vehcat_data["month"] == 0].copy()

    has_monthly_vc = not vehcat_monthly.empty

    if has_monthly_vc or not vehcat_annual.empty:
        st.subheader("Vehicle Category Breakdown")

        if has_monthly_vc:
            # -- MONTHLY MODE: use period filter + frequency selector --
            st.caption("Registration mix from Vahan portal (Y=Maker × X=Vehicle Category)")

            vc_df = vehcat_monthly.copy()
            vc_df["date"] = pd.to_datetime(
                vc_df["year"].astype(str) + "-" + vc_df["month"].astype(str).str.zfill(2) + "-01")

            vc_df = filter_by_period(vc_df, start_date, end_date)

            if not vc_df.empty:
                # Aggregate by frequency
                if freq != "monthly":
                    vc_df = add_fy_columns(vc_df)
                    if freq == "quarterly":
                        grp = ["category_group", "fy", "quarter", "q_label"]
                    else:
                        grp = ["category_group", "fy", "fy_label"]
                    vc_df = vc_df.groupby(grp).agg(volume=("volume", "sum")).reset_index()
                    if freq == "quarterly":
                        vc_df["period_label"] = vc_df["q_label"]
                        vc_df["date"] = vc_df.apply(
                            lambda r: pd.Timestamp(int(r["fy"]) + (1 if r["quarter"] == 4 else 0),
                                                   {1: 5, 2: 8, 3: 11, 4: 2}[int(r["quarter"])], 1), axis=1)
                    else:
                        vc_df["period_label"] = vc_df["fy_label"]
                        vc_df["date"] = vc_df["fy"].apply(lambda y: pd.Timestamp(int(y), 10, 1))
                else:
                    vc_df["period_label"] = vc_df.apply(
                        lambda r: format_month(int(r["year"]), int(r["month"])), axis=1)

                vc_df = vc_df.sort_values("date")
                period_labels_vc = vc_df.sort_values("date")["period_label"].unique().tolist()

                # Sort category groups by total volume
                col_order = vc_df.groupby("category_group")["volume"].sum().sort_values(ascending=False).index.tolist()

                # Compute share % within each period
                period_totals_vc = vc_df.groupby("period_label")["volume"].sum().to_dict()
                vc_df["pct"] = vc_df.apply(
                    lambda r: round(r["volume"] / period_totals_vc[r["period_label"]] * 100, 1)
                    if period_totals_vc.get(r["period_label"], 0) > 0 else 0, axis=1)

                # -- 5A: 100% Stacked Bar Chart --
                pivot_vc_pct = vc_df.pivot_table(
                    index="period_label", columns="category_group", values="pct", fill_value=0)
                pivot_vc_pct = pivot_vc_pct.reindex(period_labels_vc)
                pivot_vc_pct = pivot_vc_pct.reindex(columns=[c for c in col_order if c in pivot_vc_pct.columns])

                vc_colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3", "#FF6692", "#B6E880"]
                fig_vc = go.Figure()
                for i, col_name in enumerate(pivot_vc_pct.columns):
                    fig_vc.add_trace(go.Bar(
                        x=pivot_vc_pct.index, y=pivot_vc_pct[col_name],
                        name=col_name, marker_color=vc_colors[i % len(vc_colors)],
                    ))
                freq_title_vc = {"monthly": "Monthly", "quarterly": "Quarterly", "annual": "Annual"}[freq]
                fig_vc.update_layout(
                    **LAYOUT_DEFAULTS,
                    barmode="stack",
                    title=f"{oem} — Vehicle Category Mix ({freq_title_vc}) (%)",
                    yaxis_title="Share %",
                    height=400,
                )
                fig_vc.update_xaxes(title="")
                st.plotly_chart(fig_vc, use_container_width=True)

                # -- 5B: Volume Table --
                pivot_vc_vol = vc_df.pivot_table(
                    index="category_group", columns="period_label", values="volume", fill_value=0)
                pivot_vc_vol = pivot_vc_vol.reindex(columns=period_labels_vc)
                pivot_vc_vol = pivot_vc_vol.reindex([c for c in col_order if c in pivot_vc_vol.index])

                total_vc = pivot_vc_vol.sum(axis=0)
                total_vc.name = "TOTAL"
                pivot_vc_vol = pd.concat([pivot_vc_vol, total_vc.to_frame().T])

                display_vc_vol = pivot_vc_vol.map(
                    lambda x: _fmt_vol(x) if isinstance(x, (int, float, np.integer, np.floating)) else x)
                display_vc_vol.index.name = "Vehicle Category"

                st.markdown(f"**Vehicle Category — Volume ({freq_title_vc})**")
                st.dataframe(display_vc_vol, use_container_width=True)

                # -- 5C: YoY Growth Table --
                # Build lookup: (category_group, period_key) -> volume
                # Need full data (not just filtered period) for prior-year comparisons
                vc_full = vehcat_monthly.copy()
                vc_full["date"] = pd.to_datetime(
                    vc_full["year"].astype(str) + "-" + vc_full["month"].astype(str).str.zfill(2) + "-01")
                if freq != "monthly":
                    vc_full = add_fy_columns(vc_full)
                    if freq == "quarterly":
                        vc_full = vc_full.groupby(["category_group", "fy", "quarter"]).agg(
                            volume=("volume", "sum")).reset_index()
                    else:
                        vc_full = vc_full.groupby(["category_group", "fy"]).agg(
                            volume=("volume", "sum")).reset_index()

                vc_vol_lookup = {}
                for _, r in vc_full.iterrows():
                    cg = r["category_group"]
                    if freq == "monthly":
                        pk = (int(r["year"]), int(r["month"]))
                    elif freq == "quarterly":
                        pk = (int(r["fy"]), int(r["quarter"]))
                    else:
                        pk = int(r["fy"])
                    vc_vol_lookup[(cg, pk)] = vc_vol_lookup.get((cg, pk), 0) + r["volume"]

                growth_vc = {}
                for cg in [c for c in col_order if c in pivot_vc_vol.index] + ["TOTAL"]:
                    growth_vc[cg] = {}

                for plbl in period_labels_vc:
                    rows_p = vc_df[vc_df["period_label"] == plbl]
                    if rows_p.empty:
                        continue
                    sample = rows_p.iloc[0]

                    for cg in col_order:
                        if freq == "monthly":
                            pk = (int(sample["year"]), int(sample["month"]))
                            prev_pk = (int(sample["year"]) - 1, int(sample["month"]))
                        elif freq == "quarterly":
                            pk = (int(sample["fy"]), int(sample["quarter"]))
                            prev_pk = (int(sample["fy"]) - 1, int(sample["quarter"]))
                        else:
                            pk = int(sample["fy"])
                            prev_pk = int(sample["fy"]) - 1

                        curr = vc_vol_lookup.get((cg, pk), 0)
                        prev = vc_vol_lookup.get((cg, prev_pk), 0)
                        growth_vc.setdefault(cg, {})[plbl] = _safe_growth(curr, prev)

                    # TOTAL row
                    if freq == "monthly":
                        pk = (int(sample["year"]), int(sample["month"]))
                        prev_pk = (int(sample["year"]) - 1, int(sample["month"]))
                    elif freq == "quarterly":
                        pk = (int(sample["fy"]), int(sample["quarter"]))
                        prev_pk = (int(sample["fy"]) - 1, int(sample["quarter"]))
                    else:
                        pk = int(sample["fy"])
                        prev_pk = int(sample["fy"]) - 1

                    curr_t = sum(v for (c, k), v in vc_vol_lookup.items() if k == pk)
                    prev_t = sum(v for (c, k), v in vc_vol_lookup.items() if k == prev_pk)
                    growth_vc.setdefault("TOTAL", {})[plbl] = _safe_growth(curr_t, prev_t)

                growth_vc_df = pd.DataFrame(growth_vc).T.reindex(columns=period_labels_vc)
                growth_vc_df.index.name = "Vehicle Category"
                display_vc_growth = growth_vc_df.map(_fmt_growth)

                with st.expander(f"Vehicle Category — YoY Growth ({freq_title_vc}) (%)", expanded=False):
                    st.dataframe(display_vc_growth, use_container_width=True)

        else:
            # -- ANNUAL FALLBACK: only month=0 data available --
            st.caption("Annual registration mix from Vahan portal (Y=Maker × X=Vehicle Category)")

            vehcat_annual["fy_label"] = vehcat_annual["year"].apply(
                lambda y: f"FY{(y + 1) % 100:02d}")
            vehcat_annual = vehcat_annual.sort_values("year")

            fy_labels_ordered = vehcat_annual.sort_values("year")["fy_label"].unique().tolist()

            fy_totals = vehcat_annual.groupby("fy_label")["volume"].sum().to_dict()
            vehcat_annual["pct"] = vehcat_annual.apply(
                lambda r: round(r["volume"] / fy_totals[r["fy_label"]] * 100, 1)
                if fy_totals.get(r["fy_label"], 0) > 0 else 0, axis=1)

            col_order = vehcat_annual.groupby("category_group")["volume"].sum().sort_values(ascending=False).index.tolist()

            pivot_vc_pct = vehcat_annual.pivot_table(
                index="fy_label", columns="category_group", values="pct", fill_value=0)
            pivot_vc_pct = pivot_vc_pct.reindex(fy_labels_ordered)
            pivot_vc_pct = pivot_vc_pct.reindex(columns=[c for c in col_order if c in pivot_vc_pct.columns])

            vc_colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3", "#FF6692", "#B6E880"]
            fig_vc = go.Figure()
            for i, col_name in enumerate(pivot_vc_pct.columns):
                fig_vc.add_trace(go.Bar(
                    x=pivot_vc_pct.index, y=pivot_vc_pct[col_name],
                    name=col_name, marker_color=vc_colors[i % len(vc_colors)],
                ))
            fig_vc.update_layout(
                **LAYOUT_DEFAULTS,
                barmode="stack",
                title=f"{oem} — Vehicle Category Mix by FY (%)",
                yaxis_title="Share %",
                height=400,
            )
            fig_vc.update_xaxes(title="")
            st.plotly_chart(fig_vc, use_container_width=True)

            pivot_vc_vol = vehcat_annual.pivot_table(
                index="category_group", columns="fy_label", values="volume", fill_value=0)
            pivot_vc_vol = pivot_vc_vol.reindex(columns=fy_labels_ordered)
            pivot_vc_vol = pivot_vc_vol.reindex([c for c in col_order if c in pivot_vc_vol.index])

            total_vc = pivot_vc_vol.sum(axis=0)
            total_vc.name = "TOTAL"
            pivot_vc_vol = pd.concat([pivot_vc_vol, total_vc.to_frame().T])

            display_vc_vol = pivot_vc_vol.map(
                lambda x: _fmt_vol(x) if isinstance(x, (int, float, np.integer, np.floating)) else x)
            display_vc_vol.index.name = "Vehicle Category"

            st.markdown("**Vehicle Category — Volume by FY**")
            st.dataframe(display_vc_vol, use_container_width=True)

            growth_vc = {}
            for cat_grp in [c for c in col_order if c in pivot_vc_vol.index] + ["TOTAL"]:
                growth_vc[cat_grp] = {}
                for i, fy in enumerate(fy_labels_ordered):
                    if i == 0:
                        growth_vc[cat_grp][fy] = None
                        continue
                    prev_fy = fy_labels_ordered[i - 1]
                    curr = pivot_vc_vol.loc[cat_grp, fy] if cat_grp in pivot_vc_vol.index else 0
                    prev = pivot_vc_vol.loc[cat_grp, prev_fy] if cat_grp in pivot_vc_vol.index else 0
                    growth_vc[cat_grp][fy] = _safe_growth(curr, prev)

            growth_vc_df = pd.DataFrame(growth_vc).T.reindex(columns=fy_labels_ordered)
            growth_vc_df.index.name = "Vehicle Category"
            display_vc_growth = growth_vc_df.map(_fmt_growth)

            with st.expander("Vehicle Category — YoY Growth (%)", expanded=False):
                st.dataframe(display_vc_growth, use_container_width=True)

        st.divider()


# ════════════════════════════════════════
# SECTION 6: FUEL MIX BREAKDOWN (national scraped data)
# ════════════════════════════════════════
fuel_data = get_oem_fuel_monthly(oem_normalized)

if not fuel_data.empty:
    fuel_monthly_raw = fuel_data[fuel_data["month"] > 0].copy()
    fuel_annual = fuel_data[fuel_data["month"] == 0].copy()

    has_monthly_fuel = not fuel_monthly_raw.empty

    if has_monthly_fuel or not fuel_annual.empty:
        st.subheader("Fuel Mix Breakdown")

        if has_monthly_fuel:
            # -- MONTHLY MODE --
            st.caption("Fuel-type registration mix from Vahan portal (Y=Maker × X=Fuel)")

            fl_df = fuel_monthly_raw.copy()
            fl_df["date"] = pd.to_datetime(
                fl_df["year"].astype(str) + "-" + fl_df["month"].astype(str).str.zfill(2) + "-01")

            fl_df = filter_by_period(fl_df, start_date, end_date)

            if not fl_df.empty:
                if freq != "monthly":
                    fl_df = add_fy_columns(fl_df)
                    if freq == "quarterly":
                        grp = ["fuel_group", "fy", "quarter", "q_label"]
                    else:
                        grp = ["fuel_group", "fy", "fy_label"]
                    fl_df = fl_df.groupby(grp).agg(volume=("volume", "sum")).reset_index()
                    if freq == "quarterly":
                        fl_df["period_label"] = fl_df["q_label"]
                        fl_df["date"] = fl_df.apply(
                            lambda r: pd.Timestamp(int(r["fy"]) + (1 if r["quarter"] == 4 else 0),
                                                   {1: 5, 2: 8, 3: 11, 4: 2}[int(r["quarter"])], 1), axis=1)
                    else:
                        fl_df["period_label"] = fl_df["fy_label"]
                        fl_df["date"] = fl_df["fy"].apply(lambda y: pd.Timestamp(int(y), 10, 1))
                else:
                    fl_df["period_label"] = fl_df.apply(
                        lambda r: format_month(int(r["year"]), int(r["month"])), axis=1)

                fl_df = fl_df.sort_values("date")
                period_labels_fl = fl_df.sort_values("date")["period_label"].unique().tolist()

                fuel_col_order = fl_df.groupby("fuel_group")["volume"].sum().sort_values(ascending=False).index.tolist()

                period_totals_fl = fl_df.groupby("period_label")["volume"].sum().to_dict()
                fl_df["pct"] = fl_df.apply(
                    lambda r: round(r["volume"] / period_totals_fl[r["period_label"]] * 100, 1)
                    if period_totals_fl.get(r["period_label"], 0) > 0 else 0, axis=1)

                # -- 6A: 100% Stacked Bar Chart --
                pivot_fuel_pct = fl_df.pivot_table(
                    index="period_label", columns="fuel_group", values="pct", fill_value=0)
                pivot_fuel_pct = pivot_fuel_pct.reindex(period_labels_fl)
                pivot_fuel_pct = pivot_fuel_pct.reindex(columns=[c for c in fuel_col_order if c in pivot_fuel_pct.columns])

                fuel_colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3", "#FF6692", "#B6E880"]
                fig_fuel = go.Figure()
                for i, col_name in enumerate(pivot_fuel_pct.columns):
                    fig_fuel.add_trace(go.Bar(
                        x=pivot_fuel_pct.index, y=pivot_fuel_pct[col_name],
                        name=col_name, marker_color=fuel_colors[i % len(fuel_colors)],
                    ))
                freq_title_fl = {"monthly": "Monthly", "quarterly": "Quarterly", "annual": "Annual"}[freq]
                fig_fuel.update_layout(
                    **LAYOUT_DEFAULTS,
                    barmode="stack",
                    title=f"{oem} — Fuel Mix ({freq_title_fl}) (%)",
                    yaxis_title="Share %",
                    height=400,
                )
                fig_fuel.update_xaxes(title="")
                st.plotly_chart(fig_fuel, use_container_width=True)

                # -- 6B: Volume Table --
                pivot_fuel_vol = fl_df.pivot_table(
                    index="fuel_group", columns="period_label", values="volume", fill_value=0)
                pivot_fuel_vol = pivot_fuel_vol.reindex(columns=period_labels_fl)
                pivot_fuel_vol = pivot_fuel_vol.reindex([c for c in fuel_col_order if c in pivot_fuel_vol.index])

                total_fuel = pivot_fuel_vol.sum(axis=0)
                total_fuel.name = "TOTAL"
                pivot_fuel_vol = pd.concat([pivot_fuel_vol, total_fuel.to_frame().T])

                display_fuel_vol = pivot_fuel_vol.map(
                    lambda x: _fmt_vol(x) if isinstance(x, (int, float, np.integer, np.floating)) else x)
                display_fuel_vol.index.name = "Fuel Type"

                st.markdown(f"**Fuel Mix — Volume ({freq_title_fl})**")
                st.dataframe(display_fuel_vol, use_container_width=True)

                # -- 6C: YoY Growth Table --
                fl_full = fuel_monthly_raw.copy()
                fl_full["date"] = pd.to_datetime(
                    fl_full["year"].astype(str) + "-" + fl_full["month"].astype(str).str.zfill(2) + "-01")
                if freq != "monthly":
                    fl_full = add_fy_columns(fl_full)
                    if freq == "quarterly":
                        fl_full = fl_full.groupby(["fuel_group", "fy", "quarter"]).agg(
                            volume=("volume", "sum")).reset_index()
                    else:
                        fl_full = fl_full.groupby(["fuel_group", "fy"]).agg(
                            volume=("volume", "sum")).reset_index()

                fl_vol_lookup = {}
                for _, r in fl_full.iterrows():
                    fg = r["fuel_group"]
                    if freq == "monthly":
                        pk = (int(r["year"]), int(r["month"]))
                    elif freq == "quarterly":
                        pk = (int(r["fy"]), int(r["quarter"]))
                    else:
                        pk = int(r["fy"])
                    fl_vol_lookup[(fg, pk)] = fl_vol_lookup.get((fg, pk), 0) + r["volume"]

                growth_fuel = {}
                for fg in fuel_col_order:
                    growth_fuel[fg] = {}

                for plbl in period_labels_fl:
                    rows_p = fl_df[fl_df["period_label"] == plbl]
                    if rows_p.empty:
                        continue
                    sample = rows_p.iloc[0]

                    for fg in fuel_col_order:
                        if freq == "monthly":
                            pk = (int(sample["year"]), int(sample["month"]))
                            prev_pk = (int(sample["year"]) - 1, int(sample["month"]))
                        elif freq == "quarterly":
                            pk = (int(sample["fy"]), int(sample["quarter"]))
                            prev_pk = (int(sample["fy"]) - 1, int(sample["quarter"]))
                        else:
                            pk = int(sample["fy"])
                            prev_pk = int(sample["fy"]) - 1

                        curr = fl_vol_lookup.get((fg, pk), 0)
                        prev = fl_vol_lookup.get((fg, prev_pk), 0)
                        growth_fuel.setdefault(fg, {})[plbl] = _safe_growth(curr, prev)

                    # TOTAL row
                    if freq == "monthly":
                        pk = (int(sample["year"]), int(sample["month"]))
                        prev_pk = (int(sample["year"]) - 1, int(sample["month"]))
                    elif freq == "quarterly":
                        pk = (int(sample["fy"]), int(sample["quarter"]))
                        prev_pk = (int(sample["fy"]) - 1, int(sample["quarter"]))
                    else:
                        pk = int(sample["fy"])
                        prev_pk = int(sample["fy"]) - 1

                    curr_t = sum(v for (c, k), v in fl_vol_lookup.items() if k == pk)
                    prev_t = sum(v for (c, k), v in fl_vol_lookup.items() if k == prev_pk)
                    growth_fuel.setdefault("TOTAL", {})[plbl] = _safe_growth(curr_t, prev_t)

                growth_fuel_df = pd.DataFrame(growth_fuel).T.reindex(columns=period_labels_fl)
                growth_fuel_df.index.name = "Fuel Type"
                display_fuel_growth = growth_fuel_df.map(_fmt_growth)

                with st.expander(f"Fuel Mix — YoY Growth ({freq_title_fl}) (%)", expanded=False):
                    st.dataframe(display_fuel_growth, use_container_width=True)

        else:
            # -- ANNUAL FALLBACK --
            st.caption("Annual fuel-type registration mix from Vahan portal (Y=Maker × X=Fuel)")

            fuel_annual["fy_label"] = fuel_annual["year"].apply(
                lambda y: f"FY{(y + 1) % 100:02d}")
            fuel_annual = fuel_annual.sort_values("year")

            fy_labels_fuel = fuel_annual["fy_label"].unique().tolist()

            fy_totals_fuel = fuel_annual.groupby("fy_label")["volume"].sum().to_dict()
            fuel_annual["pct"] = fuel_annual.apply(
                lambda r: round(r["volume"] / fy_totals_fuel[r["fy_label"]] * 100, 1)
                if fy_totals_fuel.get(r["fy_label"], 0) > 0 else 0, axis=1)

            fuel_col_order = fuel_annual.groupby("fuel_group")["volume"].sum().sort_values(ascending=False).index.tolist()

            pivot_fuel_pct = fuel_annual.pivot_table(
                index="fy_label", columns="fuel_group", values="pct", fill_value=0)
            pivot_fuel_pct = pivot_fuel_pct.reindex(fy_labels_fuel)
            pivot_fuel_pct = pivot_fuel_pct.reindex(columns=[c for c in fuel_col_order if c in pivot_fuel_pct.columns])

            fuel_colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3", "#FF6692", "#B6E880"]
            fig_fuel = go.Figure()
            for i, col_name in enumerate(pivot_fuel_pct.columns):
                fig_fuel.add_trace(go.Bar(
                    x=pivot_fuel_pct.index, y=pivot_fuel_pct[col_name],
                    name=col_name, marker_color=fuel_colors[i % len(fuel_colors)],
                ))
            fig_fuel.update_layout(
                **LAYOUT_DEFAULTS,
                barmode="stack",
                title=f"{oem} — Fuel Mix by FY (%)",
                yaxis_title="Share %",
                height=400,
            )
            fig_fuel.update_xaxes(title="")
            st.plotly_chart(fig_fuel, use_container_width=True)

            pivot_fuel_vol = fuel_annual.pivot_table(
                index="fuel_group", columns="fy_label", values="volume", fill_value=0)
            pivot_fuel_vol = pivot_fuel_vol.reindex(columns=fy_labels_fuel)
            pivot_fuel_vol = pivot_fuel_vol.reindex([c for c in fuel_col_order if c in pivot_fuel_vol.index])

            total_fuel = pivot_fuel_vol.sum(axis=0)
            total_fuel.name = "TOTAL"
            pivot_fuel_vol = pd.concat([pivot_fuel_vol, total_fuel.to_frame().T])

            display_fuel_vol = pivot_fuel_vol.map(
                lambda x: _fmt_vol(x) if isinstance(x, (int, float, np.integer, np.floating)) else x)
            display_fuel_vol.index.name = "Fuel Type"

            st.markdown("**Fuel Mix — Volume by FY**")
            st.dataframe(display_fuel_vol, use_container_width=True)

            growth_fuel = {}
            for fg in [c for c in fuel_col_order if c in pivot_fuel_vol.index] + ["TOTAL"]:
                growth_fuel[fg] = {}
                for i, fy in enumerate(fy_labels_fuel):
                    if i == 0:
                        growth_fuel[fg][fy] = None
                        continue
                    prev_fy = fy_labels_fuel[i - 1]
                    curr = pivot_fuel_vol.loc[fg, fy] if fg in pivot_fuel_vol.index else 0
                    prev = pivot_fuel_vol.loc[fg, prev_fy] if fg in pivot_fuel_vol.index else 0
                    growth_fuel[fg][fy] = _safe_growth(curr, prev)

            growth_fuel_df = pd.DataFrame(growth_fuel).T.reindex(columns=fy_labels_fuel)
            growth_fuel_df.index.name = "Fuel Type"
            display_fuel_growth = growth_fuel_df.map(_fmt_growth)

            with st.expander("Fuel Mix — YoY Growth (%)", expanded=False):
                st.dataframe(display_fuel_growth, use_container_width=True)

        st.divider()

# ════════════════════════════════════════
# SECTION 7: STATE ANALYSIS (scraped data)
# ════════════════════════════════════════
if has_state_data():
    st.subheader("State Analysis")
    st.caption("State data is cross-category (all vehicle types combined for each OEM)")

    # ── 5A: State Sales Split — Horizontal Bar ──
    state_market = get_oem_state_market_shares(oem, "__ALL__", ref_year, ref_month)

    if not state_market.empty:
        top_states = state_market.head(15).copy()
        top_state_names = top_states["state"].tolist()

        fig = horizontal_bar(
            top_states, x="oem_volume", y="state",
            title=f"Top States — {oem} ({format_month(ref_year, ref_month)})",
            text=top_states["contribution_pct"].apply(lambda x: f"{x:.1f}%"),
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── 5B: State Volumes Over Time ──
        state_oem_all = get_state_oem_monthly(oem, "__ALL__")

        if not state_oem_all.empty:
            state_oem_all = filter_by_period(state_oem_all, start_date, end_date)

            if not state_oem_all.empty:
                # Aggregate by frequency per state
                if freq != "monthly":
                    state_oem_all = add_fy_columns(state_oem_all)
                    if freq == "quarterly":
                        grp = ["state", "fy", "quarter", "q_label"]
                    else:
                        grp = ["state", "fy", "fy_label"]
                    state_oem_all = state_oem_all.groupby(grp).agg(volume=("volume", "sum")).reset_index()
                    if freq == "quarterly":
                        state_oem_all["label"] = state_oem_all["q_label"]
                        state_oem_all["date"] = state_oem_all.apply(
                            lambda r: pd.Timestamp(int(r["fy"]) + (1 if r["quarter"] == 4 else 0),
                                                   {1: 5, 2: 8, 3: 11, 4: 2}[int(r["quarter"])], 1), axis=1)
                    else:
                        state_oem_all["label"] = state_oem_all["fy_label"]
                        state_oem_all["date"] = state_oem_all["fy"].apply(
                            lambda y: pd.Timestamp(int(y), 10, 1))
                else:
                    state_oem_all["label"] = state_oem_all.apply(
                        lambda r: format_month(int(r["year"]), int(r["month"])), axis=1
                    )

                ordered_months = state_oem_all.sort_values("date")["label"].unique().tolist()

                pivot_state = state_oem_all.pivot_table(
                    index="state", columns="label", values="volume", fill_value=0
                )
                # Columns chronological: oldest left, newest right
                pivot_state = pivot_state.reindex(columns=ordered_months)

                # Filter to top 15 states
                pivot_state = pivot_state.reindex([s for s in top_state_names if s in pivot_state.index])

                # Add TOTAL row
                total_row = pivot_state.sum(axis=0)
                total_row.name = "TOTAL"
                pivot_state = pd.concat([pivot_state, total_row.to_frame().T])

                # Volume table
                display_state_vol = pivot_state.map(
                    lambda x: _fmt_vol(x) if isinstance(x, (int, float, np.integer, np.floating)) else x
                )
                display_state_vol.index.name = "State"

                st.markdown("**State-wise Volume**")
                st.dataframe(display_state_vol, use_container_width=True)

                # ── Contribution % table ──
                col_totals = pivot_state.loc["TOTAL"]
                contrib_pct = pivot_state.div(col_totals).mul(100).round(1)
                # Replace NaN/inf with 0
                contrib_pct = contrib_pct.replace([np.inf, -np.inf], 0).fillna(0)
                display_contrib = contrib_pct.map(lambda x: f"{x:.1f}%" if isinstance(x, (int, float, np.floating)) else x)
                display_contrib.index.name = "State"

                with st.expander("State Contribution %", expanded=False):
                    st.dataframe(display_contrib, use_container_width=True)

                # ── 5C: State YoY Growth Table ──
                # Build per-state volumes aggregated by frequency, then compute YoY
                state_oem_full = get_state_oem_monthly(oem, "__ALL__")
                if not state_oem_full.empty:
                    # Aggregate full data (including prior years for YoY) by frequency
                    if freq != "monthly":
                        state_full_agg = add_fy_columns(state_oem_full)
                        if freq == "quarterly":
                            grp_full = ["state", "fy", "quarter", "q_label"]
                        else:
                            grp_full = ["state", "fy", "fy_label"]
                        state_full_agg = state_full_agg.groupby(grp_full).agg(
                            volume=("volume", "sum")).reset_index()
                        if freq == "quarterly":
                            state_full_agg["period_label"] = state_full_agg["q_label"]
                            state_full_agg["date"] = state_full_agg.apply(
                                lambda r: pd.Timestamp(int(r["fy"]) + (1 if r["quarter"] == 4 else 0),
                                                       {1: 5, 2: 8, 3: 11, 4: 2}[int(r["quarter"])], 1), axis=1)
                        else:
                            state_full_agg["period_label"] = state_full_agg["fy_label"]
                            state_full_agg["date"] = state_full_agg["fy"].apply(
                                lambda y: pd.Timestamp(int(y), 10, 1))
                    else:
                        state_full_agg = state_oem_full.copy()
                        state_full_agg["fy"] = state_full_agg.apply(
                            lambda r: get_fy(int(r["year"]), int(r["month"])), axis=1)
                        state_full_agg["period_label"] = state_full_agg.apply(
                            lambda r: format_month(int(r["year"]), int(r["month"])), axis=1)

                    # Get period labels in the selected date range
                    period_in_range = state_full_agg[
                        (state_full_agg["date"] >= pd.Timestamp(start_date)) &
                        (state_full_agg["date"] <= pd.Timestamp(end_date))
                    ].sort_values("date")["period_label"].unique().tolist()

                    if period_in_range:
                        # Build vol lookup: {(state, period_key): volume}
                        vol_map_state = {}
                        for _, r in state_full_agg.iterrows():
                            st_name = r["state"]
                            if freq == "monthly":
                                pk = (int(r["year"]), int(r["month"]))
                            elif freq == "quarterly":
                                pk = (int(r["fy"]), int(r["quarter"]))
                            else:
                                pk = int(r["fy"])
                            vol_map_state[(st_name, pk)] = r["volume"]

                        # For each period_label, find the period_key and prev_period_key
                        growth_data_state = {}
                        total_vols = {}
                        for plbl in period_in_range:
                            sample = state_full_agg[state_full_agg["period_label"] == plbl].iloc[0]
                            if freq == "monthly":
                                pk = (int(sample["year"]), int(sample["month"]))
                                prev_pk = (int(sample["year"]) - 1, int(sample["month"]))
                            elif freq == "quarterly":
                                pk = (int(sample["fy"]), int(sample["quarter"]))
                                prev_pk = (int(sample["fy"]) - 1, int(sample["quarter"]))
                            else:
                                pk = int(sample["fy"])
                                prev_pk = int(sample["fy"]) - 1

                            total_curr, total_prev = 0, 0
                            for st_name in top_state_names:
                                curr = vol_map_state.get((st_name, pk), 0)
                                prev = vol_map_state.get((st_name, prev_pk), 0)
                                g = _safe_growth(curr, prev)
                                growth_data_state.setdefault(st_name, {})[plbl] = g
                                total_curr += curr
                                total_prev += prev

                            growth_data_state.setdefault("TOTAL", {})[plbl] = _safe_growth(total_curr, total_prev)

                        # Build DataFrame with State as index (freezes on scroll)
                        growth_df_state = pd.DataFrame(growth_data_state).T
                        growth_df_state = growth_df_state.reindex(
                            index=[s for s in top_state_names if s in growth_df_state.index] + ["TOTAL"],
                            columns=period_in_range,
                        )
                        growth_df_state.index.name = "State"
                        display_state_growth = growth_df_state.map(_fmt_growth)

                        st.markdown("**State YoY Growth**")
                        st.dataframe(display_state_growth, use_container_width=True)
    else:
        st.info("No state data for this OEM.")
else:
    st.info("State-level data not yet available. Use the Vahan scraper to populate state data.")
