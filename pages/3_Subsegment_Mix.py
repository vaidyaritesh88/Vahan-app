"""Page 3: Subsegment Mix - ICE / EV / CNG / Hybrid analysis per base category.

Follows OEM 360 / State Performance patterns:
  - Period selector + Frequency (Monthly/Quarterly/FY)
  - Incomplete period handling
  - Transposed data tables with full numbers
  - ICE = Base - sum(subsegments), clipped to 0
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_all_categories_monthly_from_vehcat,
    get_all_subsegment_totals_monthly,
    get_all_oem_subsegment_monthly,
    get_last_scrape_info,
)
from components.filters import period_selector
from components.formatters import format_units, format_month, format_pct, OEM_COLORS
from components.charts import dual_axis_bar_line, horizontal_bar
from components.analysis import (
    aggregate_by_frequency, filter_by_period, get_period_months, add_fy_columns,
)
from config.settings import CATEGORY_CONFIG

init_db()

st.markdown("""
<style>
div[data-testid="stDataFrame"] td { text-align: right !important; }
div[data-testid="stDataFrame"] th { text-align: center !important; }
div[data-testid="stDataFrame"] table { font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)

st.title("Powertrain / Subsegment Mix")

# Subsegment color mapping
SUB_COLORS = {"ICE": "#636EFA", "EV": "#00CC96", "CNG": "#FFA15A", "Hybrid": "#AB63FA"}

# Map subsegment codes to display labels and base categories
SUBSEG_MAP = {
    "PV": {"EV_PV": "EV", "PV_CNG": "CNG", "PV_HYBRID": "Hybrid"},
    "2W": {"EV_2W": "EV"},
    "3W": {"EV_3W": "EV"},
}

BASE_CATS_WITH_SUBS = ["PV", "2W", "3W"]


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
selected_base = st.sidebar.selectbox("Base Category", BASE_CATS_WITH_SUBS, key="sm_base")
preset, ref_year, ref_month = period_selector(key="sm_period")
if ref_year is None:
    st.warning("No data loaded.")
    st.stop()

FREQ_MAP = {"Monthly": "monthly", "Quarterly": "quarterly", "Financial Year": "annual"}
freq_label = st.sidebar.selectbox("Frequency", list(FREQ_MAP.keys()), key="sm_freq")
freq = FREQ_MAP[freq_label]

st.sidebar.divider()

_scrape_info = get_last_scrape_info()
if _scrape_info:
    with st.sidebar.expander("\U0001f504 Last Scraped", expanded=False):
        for si in _scrape_info:
            stype = si["scrape_type"].replace("national_", "N:").replace("state_", "S:")
            ts = si["last_completed"][:16] if si["last_completed"] else "Never"
            st.caption(f"**{stype}** \u2014 {ts}")


# LOAD DATA
cat_data = get_all_categories_monthly_from_vehcat()
sub_data = get_all_subsegment_totals_monthly()

if cat_data.empty:
    st.info("No scraped category data available.")
    st.stop()

# Filter to selected base category
base_monthly = cat_data[cat_data["category_code"] == selected_base].copy()
if base_monthly.empty:
    st.info(f"No data for {selected_base}.")
    st.stop()

base_monthly["date"] = pd.to_datetime(
    base_monthly["year"].astype(str) + "-" + base_monthly["month"].astype(str).str.zfill(2) + "-01"
)

# Get subsegment codes for this base category
sub_codes = SUBSEG_MAP.get(selected_base, {})
sub_labels = list(sub_codes.values())  # ["EV", "CNG", "Hybrid"]

# Build combined monthly DataFrame: base + each subsegment
combined = base_monthly[["year", "month", "date", "volume"]].copy()
combined = combined.rename(columns={"volume": "base_vol"})

for sub_code, sub_label in sub_codes.items():
    sub_slice = sub_data[sub_data["subsegment_code"] == sub_code][["year", "month", "volume"]].copy()
    sub_slice = sub_slice.rename(columns={"volume": sub_label})
    combined = combined.merge(sub_slice, on=["year", "month"], how="left")

# Fill missing subsegment data with 0
for lbl in sub_labels:
    if lbl not in combined.columns:
        combined[lbl] = 0
    combined[lbl] = combined[lbl].fillna(0)

# Compute ICE = base - sum(subsegments), clipped to 0
combined["ICE"] = (combined["base_vol"] - combined[sub_labels].sum(axis=1)).clip(lower=0)

# Apply period filter
start_date, end_date = get_period_months(preset, ref_year, ref_month)
filtered = filter_by_period(combined, start_date, end_date)
if filtered.empty:
    st.info("No data for the selected period.")
    st.stop()

incomplete_periods = _get_incomplete_periods(filtered, freq)

# Sub-type column order: ICE first, then subsegments
sub_type_cols = ["ICE"] + sub_labels


# SECTION 1: KPI SNAPSHOT
st.subheader(f"{selected_base} Subsegment Analysis \u2014 {format_month(ref_year, ref_month)}")

ref_row = combined[(combined["year"] == ref_year) & (combined["month"] == ref_month)]
prev_row = combined[(combined["year"] == ref_year - 1) & (combined["month"] == ref_month)]

if not ref_row.empty:
    base_vol = ref_row["base_vol"].iloc[0]
    prev_base = prev_row["base_vol"].iloc[0] if not prev_row.empty else 0
    base_yoy = round(((base_vol / prev_base) - 1) * 100, 1) if prev_base > 0 else None

    n_cards = 1 + len(sub_type_cols)
    cols = st.columns(min(n_cards, 5))
    with cols[0]:
        delta = f"{base_yoy:+.1f}%" if base_yoy is not None else None
        st.metric(f"Total {selected_base}", format_units(base_vol), delta=delta)

    for i, st_col in enumerate(sub_type_cols):
        if i + 1 >= len(cols):
            break
        with cols[i + 1]:
            vol = ref_row[st_col].iloc[0]
            pen = (vol / base_vol * 100) if base_vol > 0 else 0
            st.metric(st_col, format_units(vol), delta=f"{pen:.1f}% pen.")

st.divider()


# SECTION 2: PENETRATION TREND
st.subheader("Subsegment Penetration Trend")

# Build long-form data for aggregation
long_frames = []
for st_col in sub_type_cols:
    df_long = filtered[["year", "month", "date", st_col, "base_vol"]].copy()
    df_long = df_long.rename(columns={st_col: "volume"})
    df_long["sub_type"] = st_col
    long_frames.append(df_long)

long_df = pd.concat(long_frames, ignore_index=True)

# Aggregate by frequency
agg_frames = []
for st_col in sub_type_cols:
    slice_df = long_df[long_df["sub_type"] == st_col].copy()
    agg = aggregate_by_frequency(slice_df, freq)
    agg["sub_type"] = st_col
    agg_frames.append(agg)

# Also aggregate base total
base_for_agg = filtered[["year", "month", "date", "base_vol"]].copy()
base_for_agg = base_for_agg.rename(columns={"base_vol": "volume"})
base_agg = aggregate_by_frequency(base_for_agg, freq)
base_agg["label"] = base_agg.apply(lambda r: _period_lbl(r, freq), axis=1)

if agg_frames:
    sub_agg = pd.concat(agg_frames, ignore_index=True)
    sub_agg["label"] = sub_agg.apply(lambda r: _period_lbl(r, freq), axis=1)

    # Merge with base totals for penetration calculation
    sub_agg = sub_agg.merge(
        base_agg[["label", "volume"]].rename(columns={"volume": "base_total"}),
        on="label", how="left"
    )
    sub_agg["penetration"] = (sub_agg["volume"] / sub_agg["base_total"] * 100).round(1)

    # Chart: bars = base volume, lines = subsegment penetration
    fig = go.Figure()

    # Base volume as bars
    base_sorted = base_agg.sort_values("date")
    fig.add_trace(go.Bar(
        x=base_sorted["label"], y=base_sorted["volume"],
        name=f"{selected_base} Volume", marker_color="#D3D3D3", opacity=0.5,
        yaxis="y",
    ))

    # Penetration lines (skip ICE)
    for idx, st_col in enumerate(sub_labels):
        st_data = sub_agg[sub_agg["sub_type"] == st_col].sort_values("date")
        if st_data.empty or st_data["penetration"].sum() == 0:
            continue
        color = SUB_COLORS.get(st_col, OEM_COLORS[idx % len(OEM_COLORS)])
        fig.add_trace(go.Scatter(
            x=st_data["label"], y=st_data["penetration"],
            mode="lines+markers", name=f"{st_col} %",
            line=dict(width=2.5, color=color),
            yaxis="y2",
        ))

    fig.update_layout(
        title=f"{selected_base} Subsegment Penetration",
        yaxis=dict(title="Volume", side="left"),
        yaxis2=dict(title="Penetration %", side="right", overlaying="y"),
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
        barmode="overlay",
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()


# SECTION 3-5: DATA TABLES
st.subheader("Subsegment Data")

# Pivot sub_agg for tables
if agg_frames:
    pivot_vol = sub_agg.pivot_table(
        index="sub_type", columns="label", values="volume", aggfunc="sum"
    )
    ordered_labels = sub_agg.sort_values("date")["label"].unique().tolist()
    pivot_vol = pivot_vol.reindex(columns=ordered_labels)

    type_order = [t for t in sub_type_cols if t in pivot_vol.index]
    pivot_vol = pivot_vol.reindex(type_order)

    # Add TOTAL row (= base category total)
    total_row = base_agg.set_index("label")["volume"].reindex(ordered_labels)
    total_row.name = "TOTAL"
    pivot_vol = pd.concat([pivot_vol, total_row.to_frame().T])

    # Table A: Volume
    st.markdown("**Volume (units)**")
    vol_display = pivot_vol.copy()
    for col in vol_display.columns:
        vol_display[col] = vol_display[col].apply(
            lambda v: f"{int(v):,}" if pd.notna(v) and v > 0 else "\u2014"
        )
    vol_display.index.name = "Subsegment"
    st.dataframe(vol_display, use_container_width=True)

    # Table B: YoY Growth %
    with st.expander("YoY Growth %"):
        yoy_data_tbl = pivot_vol.copy().astype(float)
        row_labels = type_order + ["TOTAL"]
        yoy_result = pd.DataFrame(index=row_labels, columns=ordered_labels, dtype=object)

        for sub in row_labels:
            for col_idx, col in enumerate(ordered_labels):
                if col in incomplete_periods:
                    yoy_result.loc[sub, col] = "\u2014"
                    continue

                curr = (
                    yoy_data_tbl.loc[sub, col]
                    if sub in yoy_data_tbl.index and col in yoy_data_tbl.columns
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
                    yoy_result.loc[sub, col] = "\u2014"
                    continue

                if prev_label and prev_label in yoy_data_tbl.columns:
                    prev = yoy_data_tbl.loc[sub, prev_label]
                    if pd.notna(curr) and pd.notna(prev) and prev > 0:
                        yoy_val = round(((curr / prev) - 1) * 100, 1)
                        yoy_result.loc[sub, col] = f"{yoy_val:+.1f}%"
                    else:
                        yoy_result.loc[sub, col] = "\u2014"
                else:
                    yoy_result.loc[sub, col] = "\u2014"

        yoy_result.index.name = "Subsegment"
        st.dataframe(yoy_result, use_container_width=True)

    # Table C: Penetration %
    with st.expander("Penetration %"):
        pen_display = pivot_vol.drop("TOTAL", errors="ignore").copy()
        totals = pivot_vol.loc["TOTAL"] if "TOTAL" in pivot_vol.index else pivot_vol.sum(axis=0)

        for col in pen_display.columns:
            total = totals[col]
            if pd.notna(total) and total > 0:
                pen_display[col] = pen_display[col].apply(
                    lambda v, t=total: f"{v / t * 100:.1f}%" if pd.notna(v) else "\u2014"
                )
            else:
                pen_display[col] = "\u2014"

        pen_display.index.name = "Subsegment"
        st.dataframe(pen_display, use_container_width=True)

st.divider()


# SECTION 6: TOP OEMs PER SUBSEGMENT
st.subheader("Top OEMs per Subsegment")

for sub_code, sub_label in sub_codes.items():
    oem_sub = get_all_oem_subsegment_monthly(sub_code)
    if oem_sub.empty:
        continue

    oem_sub["date"] = pd.to_datetime(
        oem_sub["year"].astype(str) + "-" + oem_sub["month"].astype(str).str.zfill(2) + "-01"
    )
    oem_sub_filtered = filter_by_period(oem_sub, start_date, end_date)
    if oem_sub_filtered.empty:
        continue

    # Aggregate over the period
    oem_totals = oem_sub_filtered.groupby("oem_name").agg(
        volume=("volume", "sum")
    ).reset_index().sort_values("volume", ascending=False)

    top_10 = oem_totals.head(10)
    total_vol = oem_totals["volume"].sum()
    top_10["share_pct"] = (top_10["volume"] / total_vol * 100).round(1) if total_vol > 0 else 0
    top_10["label"] = top_10.apply(
        lambda r: f"{int(r['volume']):,} ({r['share_pct']:.1f}%)", axis=1
    )

    cat_name = CATEGORY_CONFIG.get(sub_code, {}).get("name", sub_code)
    with st.expander(f"{cat_name} ({sub_label}) \u2014 Top OEMs"):
        col1, col2 = st.columns([3, 1])
        with col1:
            fig = horizontal_bar(top_10, x="volume", y="oem_name",
                                 title=f"Top OEMs \u2014 {cat_name}", text="label")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.markdown(f"**Total Volume:** {int(total_vol):,}")
            st.markdown(f"**OEMs with data:** {len(oem_totals)}")
            st.markdown(f"**Top 10 share:** {top_10['share_pct'].sum():.1f}%")
