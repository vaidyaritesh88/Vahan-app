"""Page 1: Category Overview - National category volumes, YoY, mix analysis.

Follows OEM 360 / State Performance patterns:
  - Period selector + Frequency (Monthly/Quarterly/FY)
  - Incomplete period handling
  - Transposed data tables with full numbers
  - Dual-axis bar+line chart
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
    get_last_scrape_info,
)
from components.filters import period_selector
from components.formatters import format_units, format_month, format_pct, OEM_COLORS
from components.charts import dual_axis_bar_line
from components.analysis import (
    aggregate_by_frequency, compute_growth_series,
    filter_by_period, get_period_months, add_fy_columns,
)

init_db()

# CSS for right-aligned tables
st.markdown("""
<style>
div[data-testid="stDataFrame"] td { text-align: right !important; }
div[data-testid="stDataFrame"] th { text-align: center !important; }
div[data-testid="stDataFrame"] table { font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)

st.title("Category Overview")

BASE_CATS = ["PV", "2W", "3W", "CV", "TRACTORS"]


# \u2500\u2500 Helpers \u2500\u2500
def _fmt_vol(val):
    if pd.isna(val) or val is None:
        return "\u2014"
    return f"{int(val):,}"


def _fmt_growth(val):
    if val is None or pd.isna(val):
        return "\u2014"
    return f"{val:+.1f}%"


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


# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
# SIDEBAR
# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
preset, ref_year, ref_month = period_selector(key="catov_period")
if ref_year is None:
    st.warning("No data loaded. Upload data or run scraper in Data Management.")
    st.stop()

FREQ_MAP = {"Monthly": "monthly", "Quarterly": "quarterly", "Financial Year": "annual"}
freq_label = st.sidebar.selectbox("Frequency", list(FREQ_MAP.keys()), key="catov_freq")
freq = FREQ_MAP[freq_label]

st.sidebar.divider()

_scrape_info = get_last_scrape_info()
if _scrape_info:
    with st.sidebar.expander("\U0001f504 Last Scraped", expanded=False):
        for si in _scrape_info:
            stype = si["scrape_type"].replace("national_", "N:").replace("state_", "S:")
            ts = si["last_completed"][:16] if si["last_completed"] else "Never"
            st.caption(f"**{stype}** \u2014 {ts}")


# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
# LOAD DATA
# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
all_data = get_all_categories_monthly_from_vehcat()
if all_data.empty:
    st.info("No scraped category data available. Run the Vahan scraper first.")
    st.stop()

all_data["date"] = pd.to_datetime(
    all_data["year"].astype(str) + "-" + all_data["month"].astype(str).str.zfill(2) + "-01"
)

start_date, end_date = get_period_months(preset, ref_year, ref_month)
filtered = filter_by_period(all_data, start_date, end_date)
if filtered.empty:
    st.info("No data for the selected period.")
    st.stop()

incomplete_periods = _get_incomplete_periods(filtered, freq)


# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
# SECTION 1: KPI SNAPSHOT
# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
st.subheader(f"Monthly Registrations \u2014 {format_month(ref_year, ref_month)}")

ref_data = all_data[(all_data["year"] == ref_year) & (all_data["month"] == ref_month)]
prev_data = all_data[(all_data["year"] == ref_year - 1) & (all_data["month"] == ref_month)]

cols = st.columns(len(BASE_CATS))
for i, cat in enumerate(BASE_CATS):
    with cols[i]:
        vol = ref_data[ref_data["category_code"] == cat]["volume"].sum()
        prev_vol = prev_data[prev_data["category_code"] == cat]["volume"].sum()
        yoy = round(((vol / prev_vol) - 1) * 100, 1) if prev_vol > 0 else None
        delta = f"{yoy:+.1f}%" if yoy is not None else None
        st.metric(cat, format_units(vol), delta=delta)

st.divider()


# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
# SECTION 2: INDUSTRY TREND (dual-axis)
# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
st.subheader("Industry Trend")

industry = filtered.groupby(["year", "month", "date"]).agg(volume=("volume", "sum")).reset_index()

if freq == "monthly":
    ind_agg = industry.sort_values("date").copy()
    ind_agg["period_label"] = ind_agg.apply(
        lambda r: format_month(int(r["year"]), int(r["month"])), axis=1
    )
else:
    ind_agg = add_fy_columns(industry)
    if freq == "quarterly":
        ind_agg = ind_agg.groupby(["fy", "quarter", "q_label"]).agg(
            volume=("volume", "sum")
        ).reset_index()
        ind_agg["date"] = ind_agg.apply(
            lambda r: pd.Timestamp(
                int(r["fy"]) + (1 if r["quarter"] == 4 else 0),
                {1: 5, 2: 8, 3: 11, 4: 2}[int(r["quarter"])], 1
            ),
            axis=1,
        )
        ind_agg["period_label"] = ind_agg["q_label"]
    else:
        ind_agg = ind_agg.groupby(["fy", "fy_label"]).agg(
            volume=("volume", "sum")
        ).reset_index()
        ind_agg["date"] = ind_agg["fy"].apply(lambda y: pd.Timestamp(int(y), 10, 1))
        ind_agg["period_label"] = ind_agg["fy_label"]

ind_agg = ind_agg.sort_values("date")

# Compute YoY from full dataset
full_industry = all_data.groupby(["year", "month", "date"]).agg(
    volume=("volume", "sum")
).reset_index()
full_industry = add_fy_columns(full_industry)


def _get_industry_yoy(row):
    if freq == "monthly":
        y, m = int(row["year"]), int(row["month"])
        prev = full_industry[
            (full_industry["year"] == y - 1) & (full_industry["month"] == m)
        ]
    elif freq == "quarterly":
        prev = full_industry[full_industry["fy"] == row["fy"] - 1]
        prev = prev[prev["quarter"] == row["quarter"]]
        prev = prev.groupby(["fy", "quarter"]).agg(volume=("volume", "sum")).reset_index()
    else:
        prev = full_industry[full_industry["fy"] == row["fy"] - 1]
        prev = prev.groupby("fy").agg(volume=("volume", "sum")).reset_index()
    if prev.empty:
        return None
    prev_vol = prev["volume"].sum()
    if prev_vol <= 0:
        return None
    return round(((row["volume"] / prev_vol) - 1) * 100, 1)


ind_agg["yoy_pct"] = ind_agg.apply(_get_industry_yoy, axis=1)

if incomplete_periods and freq != "monthly":
    ind_agg.loc[ind_agg["period_label"].isin(incomplete_periods), "yoy_pct"] = np.nan

ind_agg["label"] = ind_agg["period_label"]
chart_df = ind_agg.dropna(subset=["yoy_pct"]).copy()

if not chart_df.empty:
    fig = dual_axis_bar_line(
        chart_df, x="label", bar_y="volume", line_y="yoy_pct",
        title="Total Industry Registrations & YoY Growth",
        bar_name="Volume", line_name="YoY %",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    import plotly.express as px

    fig = px.bar(ind_agg, x="label", y="volume", title="Total Industry Registrations")
    fig.update_layout(height=420)
    st.plotly_chart(fig, use_container_width=True)

st.divider()


# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
# SECTION 3-5: DATA TABLES
# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550
st.subheader("Category Data")

agg_frames = []
for cat in BASE_CATS:
    cat_slice = filtered[filtered["category_code"] == cat].copy()
    if cat_slice.empty:
        continue
    agg = aggregate_by_frequency(cat_slice, freq)
    agg["category_code"] = cat
    agg_frames.append(agg)

if not agg_frames:
    st.info("No data for tables.")
    st.stop()

tables_agg = pd.concat(agg_frames, ignore_index=True)
tables_agg["label"] = tables_agg.apply(lambda r: _period_lbl(r, freq), axis=1)

pivot_vol = tables_agg.pivot_table(
    index="category_code", columns="label", values="volume", aggfunc="sum"
)

ordered_labels = tables_agg.sort_values("date")["label"].unique().tolist()
pivot_vol = pivot_vol.reindex(columns=ordered_labels)

cat_order = [c for c in BASE_CATS if c in pivot_vol.index]
pivot_vol = pivot_vol.reindex(cat_order)

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
vol_display.index.name = "Category"
st.dataframe(vol_display, use_container_width=True)

# Table B: YoY Growth %
st.markdown("**YoY Growth %**")
yoy_data_tbl = pivot_vol.copy().astype(float)
row_labels = cat_order + ["TOTAL"]
yoy_result = pd.DataFrame(index=row_labels, columns=ordered_labels, dtype=object)

for cat in row_labels:
    for col_idx, col in enumerate(ordered_labels):
        if col in incomplete_periods:
            yoy_result.loc[cat, col] = "\u2014"
            continue

        curr = (
            yoy_data_tbl.loc[cat, col]
            if cat in yoy_data_tbl.index and col in yoy_data_tbl.columns
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
            yoy_result.loc[cat, col] = "\u2014"
            continue

        if prev_label and prev_label in yoy_data_tbl.columns:
            prev = yoy_data_tbl.loc[cat, prev_label]
            if pd.notna(curr) and pd.notna(prev) and prev > 0:
                yoy_val = round(((curr / prev) - 1) * 100, 1)
                yoy_result.loc[cat, col] = f"{yoy_val:+.1f}%"
            else:
                yoy_result.loc[cat, col] = "\u2014"
        else:
            yoy_result.loc[cat, col] = "\u2014"

yoy_result.index.name = "Category"
st.dataframe(yoy_result, use_container_width=True)

# Table C: Category Mix %
st.markdown("**Category Mix %**")
pivot_no_total = pivot_vol.drop("TOTAL", errors="ignore")
totals = pivot_no_total.sum(axis=0)
mix_display = pivot_no_total.copy()

for col in mix_display.columns:
    total = totals[col]
    if pd.notna(total) and total > 0:
        mix_display[col] = mix_display[col].apply(
            lambda v, t=total: f"{v / t * 100:.1f}%" if pd.notna(v) else "\u2014"
        )
    else:
        mix_display[col] = "\u2014"

mix_display.index.name = "Category"
st.dataframe(mix_display, use_container_width=True)
