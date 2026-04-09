"""Page 2: Category Drilldown - OEM breakdown within a category.

Follows OEM 360 / State Performance patterns:
  - Period selector + Frequency (Monthly/Quarterly/FY)
  - Incomplete period handling
  - Transposed data tables with full numbers
  - Dual-axis bar+line chart
  - Market share line chart
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_category_oem_monthly_from_vehcat,
    get_all_categories_monthly_from_vehcat,
    get_last_scrape_info,
)
from components.filters import period_selector, top_n_selector
from components.formatters import format_units, format_month, format_pct, OEM_COLORS
from components.charts import dual_axis_bar_line, horizontal_bar
from components.analysis import (
    aggregate_by_frequency, filter_by_period, get_period_months, add_fy_columns,
)

init_db()

st.markdown("""
<style>
div[data-testid="stDataFrame"] td { text-align: right !important; }
div[data-testid="stDataFrame"] th { text-align: center !important; }
div[data-testid="stDataFrame"] table { font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)

st.title("Category Drilldown")

BASE_CATS = ["PV", "2W", "3W", "CV", "TRACTORS"]


def _period_lbl(row, freq):
    if freq != "monthly" and "period_label" in row.index:
        return row["period_label"]
    return format_month(int(row["year"]), int(row["month"]))


def _get_incomplete_periods(raw_data, freq):
    if freq == "monthly":
        return set()
    df = add_fy_columns(raw_data)
    if freq == "quarterly":
        counts = df.groupby("q_label")["month"].nunique()
        return set(counts[counts < 3].index)
    elif freq == "annual":
        counts = df.groupby("fy_label")["month"].nunique()
        return set(counts[counts < 12].index)
    return set()


# SIDEBAR
cat_kwargs = {"key": "cd_cat"}
if "cd_cat" not in st.session_state:
    cat_kwargs["index"] = BASE_CATS.index("2W") if "2W" in BASE_CATS else 0
selected_cat = st.sidebar.selectbox("Category", BASE_CATS, **cat_kwargs)
preset, ref_year, ref_month = period_selector(key="cd_period")
if ref_year is None:
    st.warning("No data loaded.")
    st.stop()

FREQ_MAP = {"Monthly": "monthly", "Quarterly": "quarterly", "Financial Year": "annual"}
freq_label = st.sidebar.selectbox("Frequency", list(FREQ_MAP.keys()), key="cd_freq")
freq = FREQ_MAP[freq_label]

top_n = top_n_selector(key="cd_topn")

st.sidebar.divider()

_scrape_info = get_last_scrape_info()
if _scrape_info:
    with st.sidebar.expander("\U0001f504 Last Scraped", expanded=False):
        for si in _scrape_info:
            stype = si["scrape_type"].replace("national_", "N:").replace("state_", "S:")
            ts = si["last_completed"][:16] if si["last_completed"] else "Never"
            st.caption(f"**{stype}** \u2014 {ts}")


# LOAD DATA
oem_monthly = get_category_oem_monthly_from_vehcat(selected_cat)
if oem_monthly.empty:
    st.info(f"No scraped data for {selected_cat}.")
    st.stop()

oem_monthly["date"] = pd.to_datetime(
    oem_monthly["year"].astype(str) + "-" + oem_monthly["month"].astype(str).str.zfill(2) + "-01"
)

start_date, end_date = get_period_months(preset, ref_year, ref_month)
filtered = filter_by_period(oem_monthly, start_date, end_date)
if filtered.empty:
    st.info("No data for the selected period.")
    st.stop()

incomplete_periods = _get_incomplete_periods(filtered, freq)

# Determine top N OEMs by reference month volume
ref_slice = oem_monthly[
    (oem_monthly["year"] == ref_year) & (oem_monthly["month"] == ref_month)
].copy()
if ref_slice.empty:
    ref_slice = filtered.groupby("oem_name").agg(volume=("volume", "sum")).reset_index()

top_oems = ref_slice.nlargest(top_n, "volume")["oem_name"].tolist()


# SECTION 1: KPI SNAPSHOT
st.subheader(f"{selected_cat} \u2014 {format_month(ref_year, ref_month)}")

ref_total = ref_slice["volume"].sum()
ref_top = ref_slice.nlargest(1, "volume")
leader_name = ref_top["oem_name"].iloc[0] if not ref_top.empty else "N/A"
leader_share = (ref_top["volume"].iloc[0] / ref_total * 100) if ref_total > 0 and not ref_top.empty else 0

prev_slice = oem_monthly[
    (oem_monthly["year"] == ref_year - 1) & (oem_monthly["month"] == ref_month)
]
prev_total = prev_slice["volume"].sum()
yoy_total = round(((ref_total / prev_total) - 1) * 100, 1) if prev_total > 0 else None

hhi = sum((v / ref_total * 100) ** 2 for v in ref_slice["volume"] if ref_total > 0) / 10000 if ref_total > 0 else 0

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Volume", format_units(ref_total))
with col2:
    st.metric("Market Leader", f"{leader_name} ({leader_share:.1f}%)")
with col3:
    st.metric("HHI", f"{hhi:.3f}")
with col4:
    delta = f"{yoy_total:+.1f}%" if yoy_total is not None else None
    st.metric("YoY Growth", delta if delta else "N/A")

st.divider()


# SECTION 2: CATEGORY VOLUME TREND (dual-axis)
st.subheader(f"{selected_cat} Volume Trend")

cat_total = filtered.groupby(["year", "month", "date"]).agg(
    volume=("volume", "sum")
).reset_index()

if freq == "monthly":
    cat_agg = cat_total.sort_values("date").copy()
    cat_agg["period_label"] = cat_agg.apply(
        lambda r: format_month(int(r["year"]), int(r["month"])), axis=1
    )
else:
    cat_agg = add_fy_columns(cat_total)
    if freq == "quarterly":
        cat_agg = cat_agg.groupby(["fy", "quarter", "q_label"]).agg(
            volume=("volume", "sum")
        ).reset_index()
        cat_agg["date"] = cat_agg.apply(
            lambda r: pd.Timestamp(
                int(r["fy"]) + (1 if r["quarter"] == 4 else 0),
                {1: 5, 2: 8, 3: 11, 4: 2}[int(r["quarter"])], 1
            ),
            axis=1,
        )
        cat_agg["period_label"] = cat_agg["q_label"]
    else:
        cat_agg = cat_agg.groupby(["fy", "fy_label"]).agg(
            volume=("volume", "sum")
        ).reset_index()
        cat_agg["date"] = cat_agg["fy"].apply(lambda y: pd.Timestamp(int(y), 10, 1))
        cat_agg["period_label"] = cat_agg["fy_label"]

cat_agg = cat_agg.sort_values("date")

# YoY from full data
full_cat = oem_monthly.groupby(["year", "month", "date"]).agg(
    volume=("volume", "sum")
).reset_index()
full_cat = add_fy_columns(full_cat)


def _get_cat_yoy(row):
    if freq == "monthly":
        y, m = int(row["year"]), int(row["month"])
        prev = full_cat[(full_cat["year"] == y - 1) & (full_cat["month"] == m)]
    elif freq == "quarterly":
        prev = full_cat[full_cat["fy"] == row["fy"] - 1]
        prev = prev[prev["quarter"] == row["quarter"]]
        prev = prev.groupby(["fy", "quarter"]).agg(volume=("volume", "sum")).reset_index()
    else:
        prev = full_cat[full_cat["fy"] == row["fy"] - 1]
        prev = prev.groupby("fy").agg(volume=("volume", "sum")).reset_index()
    if prev.empty:
        return None
    pv = prev["volume"].sum()
    if pv <= 0:
        return None
    return round(((row["volume"] / pv) - 1) * 100, 1)


cat_agg["yoy_pct"] = cat_agg.apply(_get_cat_yoy, axis=1)

if incomplete_periods and freq != "monthly":
    cat_agg.loc[cat_agg["period_label"].isin(incomplete_periods), "yoy_pct"] = np.nan

cat_agg["label"] = cat_agg["period_label"]
chart_df = cat_agg.dropna(subset=["yoy_pct"]).copy()

if not chart_df.empty:
    fig = dual_axis_bar_line(
        chart_df, x="label", bar_y="volume", line_y="yoy_pct",
        title=f"{selected_cat} Registrations & YoY Growth",
        bar_name="Volume", line_name="YoY %",
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()


# SECTION 3-5: OEM DATA TABLES
st.subheader(f"OEM Data \u2014 {selected_cat}")

# Aggregate per OEM
agg_frames = []
for oem_name in top_oems:
    oem_slice = filtered[filtered["oem_name"] == oem_name].copy()
    if oem_slice.empty:
        continue
    agg = aggregate_by_frequency(oem_slice, freq)
    agg["oem_name"] = oem_name
    agg_frames.append(agg)

# Others
others = filtered[~filtered["oem_name"].isin(top_oems)].copy()
if not others.empty:
    others_agg = aggregate_by_frequency(others, freq)
    others_agg["oem_name"] = "Others"
    agg_frames.append(others_agg)

if not agg_frames:
    st.info("No OEM data for tables.")
    st.stop()

tables_agg = pd.concat(agg_frames, ignore_index=True)
tables_agg["label"] = tables_agg.apply(lambda r: _period_lbl(r, freq), axis=1)

pivot_vol = tables_agg.pivot_table(
    index="oem_name", columns="label", values="volume", aggfunc="sum"
)

ordered_labels = tables_agg.sort_values("date")["label"].unique().tolist()
pivot_vol = pivot_vol.reindex(columns=ordered_labels)

oem_order = [o for o in top_oems if o in pivot_vol.index]
if "Others" in pivot_vol.index:
    oem_order.append("Others")
pivot_vol = pivot_vol.reindex(oem_order)

total_row = pivot_vol.sum(axis=0)
total_row.name = "TOTAL"
pivot_vol = pd.concat([pivot_vol, total_row.to_frame().T])

# Table A: Volume
st.markdown("**Volume (units)**")
vol_display = pivot_vol.copy()
for col in vol_display.columns:
    vol_display[col] = vol_display[col].apply(
        lambda v: f"{int(v):,}" if pd.notna(v) and v > 0 else "\u2014"
    )
vol_display.index.name = "OEM"
st.dataframe(vol_display, use_container_width=True)

# Table B: YoY Growth %
with st.expander("YoY Growth %"):
    yoy_data_tbl = pivot_vol.copy().astype(float)
    row_labels = oem_order + ["TOTAL"]
    yoy_result = pd.DataFrame(index=row_labels, columns=ordered_labels, dtype=object)

    for oem in row_labels:
        for col_idx, col in enumerate(ordered_labels):
            if col in incomplete_periods:
                yoy_result.loc[oem, col] = "\u2014"
                continue

            curr = (
                yoy_data_tbl.loc[oem, col]
                if oem in yoy_data_tbl.index and col in yoy_data_tbl.columns
                else None
            )

            if freq == "monthly" and col_idx >= 12:
                prev_label = ordered_labels[col_idx - 12]
            elif freq == "quarterly" and col_idx >= 4:
                prev_label = ordered_labels[col_idx - 4]
            elif freq == "annual" and col_idx >= 1:
                prev_label = ordered_labels[col_idx - 1]
            else:
                prev_label = None

            if prev_label and prev_label in incomplete_periods:
                yoy_result.loc[oem, col] = "\u2014"
                continue

            if prev_label and prev_label in yoy_data_tbl.columns:
                prev = yoy_data_tbl.loc[oem, prev_label]
                if pd.notna(curr) and pd.notna(prev) and prev > 0:
                    yoy_val = round(((curr / prev) - 1) * 100, 1)
                    yoy_result.loc[oem, col] = f"{yoy_val:+.1f}%"
                else:
                    yoy_result.loc[oem, col] = "\u2014"
            else:
                yoy_result.loc[oem, col] = "\u2014"

    yoy_result.index.name = "OEM"
    st.dataframe(yoy_result, use_container_width=True)

# Table C: Market Share %
with st.expander("Market Share %"):
    share_display = pivot_vol.drop("TOTAL", errors="ignore").copy()
    totals = pivot_vol.loc["TOTAL"] if "TOTAL" in pivot_vol.index else pivot_vol.sum(axis=0)

    for col in share_display.columns:
        total = totals[col]
        if pd.notna(total) and total > 0:
            share_display[col] = share_display[col].apply(
                lambda v, t=total: f"{v / t * 100:.1f}%" if pd.notna(v) else "\u2014"
            )
        else:
            share_display[col] = "\u2014"

    share_display.index.name = "OEM"
    st.dataframe(share_display, use_container_width=True)

st.divider()


# SECTION 6: MARKET SHARE TREND (line chart)
st.subheader(f"Market Share Trend \u2014 {selected_cat}")

# Compute share per period for top OEMs
share_frames = []
for oem_name in top_oems[:7]:  # Limit to 7 for readability
    oem_slice = filtered[filtered["oem_name"] == oem_name].copy()
    if oem_slice.empty:
        continue
    oem_agg = aggregate_by_frequency(oem_slice, freq)
    oem_agg["oem_name"] = oem_name
    share_frames.append(oem_agg)

if share_frames:
    share_df = pd.concat(share_frames, ignore_index=True)
    share_df["label"] = share_df.apply(lambda r: _period_lbl(r, freq), axis=1)

    # Merge with category totals
    cat_totals = cat_agg[["label", "volume"]].rename(columns={"volume": "cat_total"})
    share_df = share_df.merge(cat_totals, on="label", how="left")
    share_df["share_pct"] = (share_df["volume"] / share_df["cat_total"] * 100).round(1)

    fig_share = go.Figure()
    for idx, oem_name in enumerate(top_oems[:7]):
        oem_d = share_df[share_df["oem_name"] == oem_name].sort_values("date")
        if oem_d.empty:
            continue
        fig_share.add_trace(go.Scatter(
            x=oem_d["label"], y=oem_d["share_pct"],
            mode="lines+markers", name=oem_name,
            line=dict(width=2, color=OEM_COLORS[idx % len(OEM_COLORS)]),
        ))

    fig_share.update_layout(
        title=f"{selected_cat} Market Share (%)",
        yaxis_title="Share %",
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
    )
    st.plotly_chart(fig_share, use_container_width=True)
