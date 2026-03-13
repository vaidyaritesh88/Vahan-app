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
)
from components.filters import oem_selector, period_selector
from components.formatters import (
    format_units, format_month, format_pct, OEM_COLORS,
    get_fy_start_year, get_fy_label,
)
from components.charts import (
    dual_axis_bar_line, line_chart, horizontal_bar, LAYOUT_DEFAULTS,
)
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

latest_year, latest_month = get_latest_month()
start_date, end_date = get_period_months(preset, ref_year, ref_month)

# ──────────────────────────────────────
# LOAD DATA
# ──────────────────────────────────────
oem_data = get_oem_monthly_all(oem)
if oem_data.empty:
    st.info(f"No data found for **{oem}**.")
    st.stop()

base_cats = get_oem_categories_list(oem)
cat_data = get_oem_all_categories(oem, ref_year, ref_month)
if cat_data.empty:
    st.warning(f"No data for {oem} in {format_month(ref_year, ref_month)}. Try a different month.")
    st.stop()

# Aggregate monthly totals — BASE categories only
base_only = oem_data[oem_data["is_subsegment"] == 0]
agg_monthly = base_only.groupby(["year", "month", "date"])["volume"].sum().reset_index()
total_vol = cat_data[cat_data["category_code"].isin(
    base_cats["category_code"] if not base_cats.empty else []
)]["volume"].sum()

st.subheader(f"{oem}")


# ════════════════════════════════════════
# SECTION 1: KPI SNAPSHOT
# ════════════════════════════════════════
growth = compute_growth_rates(agg_monthly, ref_year, ref_month)

# FYTD: only meaningful if the latest data month belongs to the CURRENT
# (ongoing) fiscal year.  E.g. in mid-April the current FY is FY27
# (Apr 2026+), but if latest data is Mar 2026 that is FY26 (previous FY)
# so FYTD for the new FY has zero completed months -> show N/A.
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
    st.metric("YoY Growth", format_pct(growth["yoy_pct"]))
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
    trend_agg = aggregate_by_frequency(trend, freq)
    trend_agg = trend_agg.sort_values("date")

    # Compute YoY growth on aggregated data
    if freq == "monthly":
        trend_agg = compute_growth_series(trend_agg)
    else:
        # For quarterly/annual: compare same period previous year
        vol_by_key = {}
        for _, r in trend_agg.iterrows():
            fy = int(r["fy"])
            if freq == "quarterly":
                key = (fy, int(r["quarter"]))
                prev_key = (fy - 1, int(r["quarter"]))
            else:
                key = fy
                prev_key = fy - 1
            vol_by_key[key] = r["volume"]

        yoy_vals = []
        for _, r in trend_agg.iterrows():
            fy = int(r["fy"])
            if freq == "quarterly":
                prev_key = (fy - 1, int(r["quarter"]))
            else:
                prev_key = fy - 1
            prev_vol = vol_by_key.get(prev_key)
            yoy_vals.append(_pct_change(r["volume"], prev_vol) if prev_vol else None)
        trend_agg["yoy_pct"] = yoy_vals

    chart_df = trend_agg.dropna(subset=["yoy_pct"]).copy()
    if not chart_df.empty:
        freq_title = {"monthly": "Monthly", "quarterly": "Quarterly", "annual": "Annual"}[freq]
        fig = dual_axis_bar_line(
            chart_df, x="date", bar_y="volume", line_y="yoy_pct",
            title=f"{oem} — {freq_title} Volume & YoY Growth",
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()


# ════════════════════════════════════════
# SECTION 3: SUB-CATEGORY SALES MIX
# ════════════════════════════════════════
if not base_cats.empty:
    st.subheader("Sub-Category Sales Mix")

    for _, bcat in base_cats.iterrows():
        base_code = bcat["category_code"]
        base_name = bcat["category_name"]

        base_monthly = oem_data[oem_data["category_code"] == base_code].copy()
        if base_monthly.empty:
            continue

        base_ref = base_monthly[
            (base_monthly["year"] == ref_year) & (base_monthly["month"] == ref_month)
        ]
        if base_ref.empty or base_ref["volume"].iloc[0] < 10:
            continue

        subs_df = get_subsegments_for_base(base_code)
        if subs_df.empty:
            continue

        sub_codes = subs_df["code"].tolist()
        sub_names = dict(zip(subs_df["code"], subs_df["name"]))

        st.markdown(f"#### {base_name}")

        # ── Build mix DataFrame ──
        base_series = base_monthly[["year", "month", "date", "volume"]].copy()
        base_series = base_series.rename(columns={"volume": "base_vol"})
        base_series = filter_by_period(base_series, start_date, end_date)

        mix_df = base_series.copy()
        for sc in sub_codes:
            sc_data = oem_data[oem_data["category_code"] == sc][["year", "month", "volume"]].copy()
            sc_data = sc_data.rename(columns={"volume": sc})
            mix_df = mix_df.merge(sc_data, on=["year", "month"], how="left")
            mix_df[sc] = mix_df[sc].fillna(0)

        mix_df["ICE"] = mix_df["base_vol"] - mix_df[sub_codes].sum(axis=1)
        mix_df["ICE"] = mix_df["ICE"].clip(lower=0)

        # ── Aggregate by frequency ──
        vol_cols_to_agg = ["base_vol", "ICE"] + sub_codes
        if freq != "monthly":
            mix_df = add_fy_columns(mix_df)
            if freq == "quarterly":
                grp = ["fy", "quarter", "q_label"]
            else:
                grp = ["fy", "fy_label"]
            agg_dict = {c: "sum" for c in vol_cols_to_agg}
            mix_df = mix_df.groupby(grp).agg(agg_dict).reset_index()
            if freq == "quarterly":
                mix_df["period_label"] = mix_df["q_label"]
                mix_df["date"] = mix_df.apply(
                    lambda r: pd.Timestamp(int(r["fy"]) + (1 if r["quarter"] == 4 else 0),
                                           {1: 5, 2: 8, 3: 11, 4: 2}[int(r["quarter"])], 1), axis=1)
            else:
                mix_df["period_label"] = mix_df["fy_label"]
                mix_df["date"] = mix_df["fy"].apply(lambda y: pd.Timestamp(int(y), 10, 1))
            mix_df = mix_df.sort_values("date")
        else:
            mix_df["period_label"] = mix_df.apply(
                lambda r: format_month(int(r["year"]), int(r["month"])), axis=1)

        fuel_types = ["ICE"] + sub_codes
        fuel_labels = {"ICE": f"ICE {base_name}"}
        fuel_labels.update({sc: sub_names.get(sc, sc) for sc in sub_codes})

        long_rows = []
        for _, row in mix_df.iterrows():
            base_total = row["base_vol"]
            if base_total <= 0:
                continue
            for ft in fuel_types:
                vol = row[ft]
                pct = round(vol / base_total * 100, 1) if base_total > 0 else 0
                entry = {
                    "date": row["date"],
                    "period_label": row["period_label"],
                    "type": fuel_labels.get(ft, ft),
                    "volume": vol,
                    "pct": pct,
                }
                # Keep fy/quarter for YoY lookup
                if "fy" in row.index:
                    entry["fy"] = int(row["fy"])
                if "quarter" in row.index:
                    entry["quarter"] = int(row["quarter"])
                if "year" in row.index:
                    entry["year"] = int(row["year"])
                if "month" in row.index:
                    entry["month"] = int(row["month"])
                long_rows.append(entry)

        if not long_rows:
            continue

        long_df = pd.DataFrame(long_rows)

        # ── 3A: 100% Stacked Bar Chart ──
        pivot_pct = long_df.pivot_table(index="date", columns="type", values="pct", fill_value=0)
        pivot_pct = pivot_pct.sort_index()

        colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3"]
        fig = go.Figure()
        for i, col_name in enumerate(pivot_pct.columns):
            fig.add_trace(go.Bar(
                x=pivot_pct.index, y=pivot_pct[col_name],
                name=col_name, marker_color=colors[i % len(colors)],
            ))
        fig.update_layout(
            **LAYOUT_DEFAULTS,
            barmode="stack",
            title=f"{oem} — {base_name} Mix (%)",
            yaxis_title="Share %",
            height=400,
        )
        fig.update_xaxes(title="")
        st.plotly_chart(fig, use_container_width=True)

        # ── 3B: Sub-Category Volume Table ──
        # Use period_label for column headers
        period_labels_ordered = long_df.sort_values("date")["period_label"].unique().tolist()

        pivot_vol = long_df.pivot_table(index="type", columns="period_label",
                                        values="volume", fill_value=0)
        pivot_vol = pivot_vol.reindex(columns=period_labels_ordered)

        # Add TOTAL row
        total_row = pivot_vol.sum(axis=0)
        total_row.name = "TOTAL"
        pivot_vol = pd.concat([pivot_vol, total_row.to_frame().T])

        # Format with commas
        display_vol = pivot_vol.map(lambda x: _fmt_vol(x) if isinstance(x, (int, float, np.integer, np.floating)) else x)
        display_vol.index.name = "Sub-Category"

        st.markdown(f"**{base_name} — Volume by Sub-Category**")
        st.dataframe(display_vol, use_container_width=True)

        # ── 3C: Sub-Category YoY Growth Table ──
        # Build volume lookup keyed by (type, period_key) for YoY comparison
        # For monthly: period_key = (year, month), prev = (year-1, month)
        # For quarterly: period_key = (fy, quarter), prev = (fy-1, quarter)
        # For annual: period_key = fy, prev = fy-1
        vol_lookup = {}  # {(type, period_key): volume}
        for _, r in long_df.iterrows():
            ft_type = r["type"]
            if freq == "monthly":
                pk = (int(r["year"]), int(r["month"]))
            elif freq == "quarterly":
                pk = (int(r["fy"]), int(r["quarter"]))
            else:
                pk = int(r["fy"])
            vol_lookup[(ft_type, pk)] = vol_lookup.get((ft_type, pk), 0) + r["volume"]

        growth_data = {}
        for ft_label in list(fuel_labels.values()) + ["TOTAL"]:
            growth_data[ft_label] = {}

        for plabel in period_labels_ordered:
            rows_for_period = long_df[long_df["period_label"] == plabel]
            if rows_for_period.empty:
                continue
            sample = rows_for_period.iloc[0]

            for ft_label in fuel_labels.values():
                if freq == "monthly":
                    pk = (int(sample["year"]), int(sample["month"]))
                    prev_pk = (int(sample["year"]) - 1, int(sample["month"]))
                elif freq == "quarterly":
                    pk = (int(sample["fy"]), int(sample["quarter"]))
                    prev_pk = (int(sample["fy"]) - 1, int(sample["quarter"]))
                else:
                    pk = int(sample["fy"])
                    prev_pk = int(sample["fy"]) - 1

                curr_vol = vol_lookup.get((ft_label, pk), 0)
                prev_vol = vol_lookup.get((ft_label, prev_pk), 0)
                g = _safe_growth(curr_vol, prev_vol) if prev_vol > 0 else None
                growth_data[ft_label][plabel] = g

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

            curr_total = sum(v for (t, k), v in vol_lookup.items() if k == pk)
            prev_total = sum(v for (t, k), v in vol_lookup.items() if k == prev_pk)
            growth_data["TOTAL"][plabel] = _safe_growth(curr_total, prev_total)

        growth_df = pd.DataFrame(growth_data).T
        growth_df = growth_df.reindex(columns=period_labels_ordered)
        growth_df.index.name = "Sub-Category"

        display_growth = growth_df.map(_fmt_growth)

        st.markdown(f"**{base_name} — YoY Growth (%)**")
        st.dataframe(display_growth, use_container_width=True)

    st.divider()


# ════════════════════════════════════════
# SECTION 4: MARKET SHARE TRENDS
# ════════════════════════════════════════
st.subheader("Market Share")

if base_cats.empty:
    st.info("No base category data for market share analysis.")
else:
    for _, bcat in base_cats.iterrows():
        base_code = bcat["category_code"]
        base_name = bcat["category_name"]

        base_ref_data = oem_data[
            (oem_data["category_code"] == base_code) &
            (oem_data["year"] == ref_year) &
            (oem_data["month"] == ref_month)
        ]
        if base_ref_data.empty:
            continue

        st.markdown(f"#### {base_name}")

        # Overall share in base category
        share_data = get_oem_with_market_totals(oem, base_code)
        if share_data.empty:
            st.info(f"No share data for {base_name}.")
            continue

        share_data = filter_by_period(share_data, start_date, end_date)
        if share_data.empty:
            continue

        # Aggregate by frequency
        share_agg = aggregate_by_frequency(share_data, freq, vol_col="oem_volume")
        share_agg = share_agg.sort_values("date")

        # Build combined share chart (overall + subsegments)
        traces_df = share_agg[["date", "share_pct"]].copy()
        traces_df["type"] = f"Overall {base_name}"

        subs_df = get_subsegments_for_base(base_code)
        for _, sub in subs_df.iterrows():
            sub_share = get_oem_with_market_totals(oem, sub["code"])
            if not sub_share.empty:
                sub_share = filter_by_period(sub_share, start_date, end_date)
                sub_share_agg = aggregate_by_frequency(sub_share, freq, vol_col="oem_volume")
                if not sub_share_agg.empty:
                    sub_trace = sub_share_agg[["date", "share_pct"]].copy()
                    sub_trace["type"] = sub["name"]
                    traces_df = pd.concat([traces_df, sub_trace], ignore_index=True)

        if not traces_df.empty:
            fig = line_chart(traces_df, x="date", y="share_pct", color="type",
                             title=f"{oem} — {base_name} Market Share", height=400)
            fig.update_yaxes(title="Market Share %")
            st.plotly_chart(fig, use_container_width=True)

        # ── Market Share Data Table (rows = share types, cols = periods) ──
        share_table_data = {}
        _share_period_order = []

        # Overall share row
        for _, r in share_agg.iterrows():
            lbl = r["period_label"] if "period_label" in r.index and freq != "monthly" else format_month(int(r["year"]), int(r["month"]))
            if lbl not in _share_period_order:
                _share_period_order.append(lbl)
            share_table_data.setdefault(f"Overall {base_name}", {})[lbl] = round(r["share_pct"], 1)

        # Subsegment share rows
        for _, sub in subs_df.iterrows():
            sub_share_raw = get_oem_with_market_totals(oem, sub["code"])
            if not sub_share_raw.empty:
                sub_share_raw = filter_by_period(sub_share_raw, start_date, end_date)
                sub_agg = aggregate_by_frequency(sub_share_raw, freq, vol_col="oem_volume")
                sub_agg = sub_agg.sort_values("date")
                for _, r in sub_agg.iterrows():
                    lbl = r["period_label"] if "period_label" in r.index and freq != "monthly" else format_month(int(r["year"]), int(r["month"]))
                    share_table_data.setdefault(sub["name"], {})[lbl] = round(r["share_pct"], 1)

        if share_table_data:
            share_tbl_df = pd.DataFrame(share_table_data).T
            share_tbl_df = share_tbl_df.reindex(columns=_share_period_order)
            share_tbl_df.index.name = "Category"
            display_share_tbl = share_tbl_df.map(
                lambda x: f"{x:.1f}%" if isinstance(x, (int, float, np.integer, np.floating)) and not pd.isna(x) else "\u2014"
            )
            st.markdown(f"**{base_name} \u2014 Market Share (%)**")
            st.dataframe(display_share_tbl, use_container_width=True)

st.divider()


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
