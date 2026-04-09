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
oem = oem_selector(key="oem360", default_oem="Hero MotoCorp")
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
                # Build full (unfiltered) long-form data for prior-year comparisons
                _sub_full_raw = get_oem_subsegment_monthly(oem_normalized, bc_code)
                _bc_full_raw = get_oem_category_monthly_scraped(oem_normalized)
                _bc_full_raw = _bc_full_raw[_bc_full_raw["base_category"] == bc_code].copy()

                if not _sub_full_raw.empty and not _bc_full_raw.empty:
                    # Build full long-form data (all months, not filtered by period)
                    _months_full = _bc_full_raw[["year", "month", "volume"]].copy()
                    _months_full = _months_full.rename(columns={"volume": "base_vol"})
                    _sub_piv_full = _sub_full_raw.pivot_table(
                        index=["year", "month"], columns="subsegment_code",
                        values="volume", fill_value=0
                    ).reset_index()
                    _comb_full = _months_full.merge(_sub_piv_full, on=["year", "month"], how="left")
                    for _sc in sub_codes:
                        if _sc not in _comb_full.columns:
                            _comb_full[_sc] = 0
                        _comb_full[_sc] = _comb_full[_sc].fillna(0)
                    _comb_full["ICE"] = (_comb_full["base_vol"] - _comb_full[sub_codes].sum(axis=1)).clip(lower=0)
                    _comb_full["date"] = pd.to_datetime(
                        _comb_full["year"].astype(str) + "-" + _comb_full["month"].astype(str).str.zfill(2) + "-01")

                    # Build long-form rows for all sub-types
                    _full_rows = []
                    for _, _r in _comb_full.iterrows():
                        for _tc in sub_type_cols:
                            _full_rows.append({
                                "year": int(_r["year"]), "month": int(_r["month"]),
                                "date": _r["date"],
                                "sub_type": _tc,
                                "sub_name": sub_type_names.get(_tc, _tc),
                                "volume": _r[_tc],
                            })
                    _full_long = pd.DataFrame(_full_rows)

                    # Aggregate by frequency
                    if freq != "monthly":
                        _full_long = add_fy_columns(_full_long)
                        if freq == "quarterly":
                            _full_long = _full_long.groupby(["sub_type", "sub_name", "fy", "quarter"]).agg(
                                volume=("volume", "sum")).reset_index()
                        else:
                            _full_long = _full_long.groupby(["sub_type", "sub_name", "fy"]).agg(
                                volume=("volume", "sum")).reset_index()

                    # Build vol lookup: (sub_type, period_key) -> volume
                    _vol_lk = {}
                    for _, _r in _full_long.iterrows():
                        if freq == "monthly":
                            _pk = (int(_r["year"]), int(_r["month"]))
                        elif freq == "quarterly":
                            _pk = (int(_r["fy"]), int(_r["quarter"]))
                        else:
                            _pk = int(_r["fy"])
                        _vol_lk[(_r["sub_type"], _pk)] = _vol_lk.get((_r["sub_type"], _pk), 0) + _r["volume"]

                    # Build growth for each period_label
                    growth_sub = {}
                    for _tc in type_order:
                        _dn = display_names.get(_tc, _tc)
                        growth_sub[_dn] = {}
                    growth_sub[f"TOTAL ({bc_name})"] = {}

                    for plbl in period_labels_sub:
                        rows_p = sub_long[sub_long["period_label"] == plbl]
                        if rows_p.empty:
                            continue
                        sample = rows_p.iloc[0]

                        if freq == "monthly":
                            _pk = (int(sample["year"]), int(sample["month"]))
                            _prev_pk = (int(sample["year"]) - 1, int(sample["month"]))
                        elif freq == "quarterly":
                            _pk = (int(sample["fy"]), int(sample["quarter"]))
                            _prev_pk = (int(sample["fy"]) - 1, int(sample["quarter"]))
                        else:
                            _pk = int(sample["fy"])
                            _prev_pk = int(sample["fy"]) - 1

                        total_curr, total_prev = 0, 0
                        for _tc in type_order:
                            _dn = display_names.get(_tc, _tc)
                            _curr = _vol_lk.get((_tc, _pk), 0)
                            _prev = _vol_lk.get((_tc, _prev_pk), 0)
                            growth_sub[_dn][plbl] = _safe_growth(_curr, _prev)
                            total_curr += _curr
                            total_prev += _prev

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
        for bc in active_cats:
            st.markdown(f"#### {bc} Market Share")

            # ── 4A: Base category share ──
            oem_bc = cat_monthly[cat_monthly["base_category"] == bc][["year", "month", "date", "volume"]].copy()
            mkt_bc = market_monthly[market_monthly["base_category"] == bc][["year", "month", "volume"]].copy()
            mkt_bc = mkt_bc.rename(columns={"volume": "market_vol"})

            merged = oem_bc.merge(mkt_bc, on=["year", "month"], how="inner")
            merged["share_pct"] = (merged["volume"] / merged["market_vol"] * 100).round(2)
            merged["label"] = f"{bc} Overall"
            merged = filter_by_period(merged, start_date, end_date)

            all_share_series = [merged] if not merged.empty else []

            # ── 4B: Subsegment share lines ──
            subs_for_bc = get_subsegments_for_base(bc)
            sub_share_colors = {
                "EV_PV": "#00CC96", "EV_2W": "#00CC96", "EV_3W": "#00CC96",
                "PV_CNG": "#FFA15A", "PV_HYBRID": "#AB63FA",
            }
            if not subs_for_bc.empty and has_oem_subsegment_data(oem_normalized):
                sub_names_map = dict(zip(subs_for_bc["code"], subs_for_bc["name"]))
                oem_sub_data = get_oem_subsegment_monthly(oem_normalized, bc)

                for _, sub_row in subs_for_bc.iterrows():
                    sc = sub_row["code"]
                    # OEM volumes for this subsegment
                    oem_sc = oem_sub_data[oem_sub_data["subsegment_code"] == sc].copy()
                    if oem_sc.empty:
                        continue
                    oem_sc = oem_sc.rename(columns={"volume": "oem_vol"})

                    # Market totals for this subsegment
                    mkt_sc = get_subsegment_market_monthly(sc)
                    if mkt_sc.empty:
                        continue

                    sc_merged = oem_sc.merge(mkt_sc, on=["year", "month"], how="inner")
                    sc_merged["share_pct"] = (
                        sc_merged["oem_vol"] / sc_merged["total_volume"] * 100
                    ).round(2)
                    sc_merged["volume"] = sc_merged["oem_vol"]
                    sc_merged["market_vol"] = sc_merged["total_volume"]
                    sc_merged["date"] = pd.to_datetime(
                        sc_merged["year"].astype(str) + "-" +
                        sc_merged["month"].astype(str).str.zfill(2) + "-01"
                    )
                    sc_merged["label"] = sub_names_map.get(sc, sc)
                    sc_merged = filter_by_period(sc_merged, start_date, end_date)
                    if not sc_merged.empty:
                        all_share_series.append(sc_merged)

            if all_share_series:
                share_df = pd.concat(all_share_series, ignore_index=True)
                share_df = share_df.sort_values("date")

                # Chart: one line per series
                fig_share = go.Figure()
                base_color = "#636EFA"
                for i, lbl in enumerate(share_df["label"].unique()):
                    lbl_data = share_df[share_df["label"] == lbl].sort_values("date")
                    if lbl_data.empty:
                        continue
                    # Base category: solid thick line. Subsegments: dashed thinner
                    is_base = lbl.endswith("Overall")
                    # Pick color
                    if is_base:
                        color = base_color
                        width = 2.5
                        dash = None
                    else:
                        # Match subsegment code to color
                        sc_match = [sc for sc in sub_share_colors if sc in lbl or sub_names_map.get(sc, "") == lbl]
                        if sc_match:
                            color = sub_share_colors[sc_match[0]]
                        else:
                            fallback = ["#EF553B", "#19D3F3", "#FF6692", "#B6E880"]
                            color = fallback[i % len(fallback)]
                        width = 2
                        dash = "dash"

                    fig_share.add_trace(go.Scatter(
                        x=lbl_data["date"], y=lbl_data["share_pct"],
                        mode="lines+markers", name=lbl,
                        line=dict(width=width, color=color, dash=dash),
                        marker=dict(size=4 if not is_base else 5),
                    ))

                fig_share.update_layout(
                    **LAYOUT_DEFAULTS,
                    title=f"{oem_normalized} — {bc} Market Share (%)",
                    yaxis_title="Market Share %",
                    height=420,
                    hovermode="x unified",
                )
                fig_share.update_xaxes(title="")
                st.plotly_chart(fig_share, use_container_width=True)

                # ── 4C: Share Data Table ──
                # Build table: rows = series (Overall, EV_PV, PV_CNG, ...), columns = months
                # Values = share %
                share_df["period_label"] = share_df.apply(
                    lambda r: format_month(int(r["year"]), int(r["month"])), axis=1)

                share_period_labels = share_df.sort_values("date")["period_label"].unique().tolist()

                # Share % pivot
                share_pivot = share_df.pivot_table(
                    index="label", columns="period_label", values="share_pct", fill_value=0)
                share_pivot = share_pivot.reindex(columns=share_period_labels)

                # Order: base category first, then subsegments
                label_order = [l for l in share_df["label"].unique() if l in share_pivot.index]
                share_pivot = share_pivot.reindex([l for l in label_order if l in share_pivot.index])

                display_share = share_pivot.map(lambda x: f"{x:.1f}%" if isinstance(x, (int, float, np.floating)) else x)
                display_share.index.name = "Segment"

                with st.expander(f"{bc} — Share % Table", expanded=False):
                    st.dataframe(display_share, use_container_width=True)

                # Volume detail table: OEM Vol / Market Total per series per month
                vol_rows_list = []
                for lbl in label_order:
                    lbl_data = share_df[share_df["label"] == lbl]
                    for _, r in lbl_data.iterrows():
                        plbl = format_month(int(r["year"]), int(r["month"]))
                        vol_rows_list.append({
                            "Segment": lbl, "Period": plbl,
                            "OEM Vol": int(r["volume"]) if not pd.isna(r["volume"]) else 0,
                            "Market Total": int(r["market_vol"]) if not pd.isna(r.get("market_vol", 0)) else 0,
                        })

                if vol_rows_list:
                    vol_detail = pd.DataFrame(vol_rows_list)

                    # Pivot OEM Vol
                    oem_vol_pivot = vol_detail.pivot_table(
                        index="Segment", columns="Period", values="OEM Vol", fill_value=0)
                    oem_vol_pivot = oem_vol_pivot.reindex(columns=share_period_labels)
                    oem_vol_pivot = oem_vol_pivot.reindex([l for l in label_order if l in oem_vol_pivot.index])

                    display_oem_vol = oem_vol_pivot.map(
                        lambda x: _fmt_vol(x) if isinstance(x, (int, float, np.integer, np.floating)) else x)
                    display_oem_vol.index.name = "Segment"

                    with st.expander(f"{bc} — OEM Volume Detail", expanded=False):
                        st.dataframe(display_oem_vol, use_container_width=True)

            st.markdown("---")

    st.divider()


# (Sections 5-6 removed: Vehicle Category Breakdown and Fuel Mix are now
#  covered by Section 3 (Category Sales Mix) and Section 3B (Sub-Category Mix))


# ════════════════════════════════════════
# SECTION 5: STATE ANALYSIS (scraped data)
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
