"""Page 9: Primary Sales - Wholesale/dispatch sales analysis for Indian auto OEMs.

Follows Category Overview / Category Drilldown patterns:
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
    get_primary_category_monthly, get_primary_oem_monthly,
    get_primary_segment_monthly, get_primary_import_stats,
    has_primary_data, get_last_scrape_info,
)
from components.filters import period_selector, top_n_selector
from components.formatters import format_units, format_month, format_pct, OEM_COLORS
from components.charts import dual_axis_bar_line
from components.analysis import (
    aggregate_by_frequency, filter_by_period, get_period_months, add_fy_columns,
)

init_db()

st.set_page_config(page_title="Primary Sales", page_icon="\U0001f4e6", layout="wide")

# CSS for right-aligned tables
st.markdown("""
<style>
div[data-testid="stDataFrame"] td { text-align: right !important; }
div[data-testid="stDataFrame"] th { text-align: center !important; }
div[data-testid="stDataFrame"] table { font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)

st.title("Primary Sales")

CATEGORIES = ["PV", "2W"]


# -- Helpers --
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


# ====================
# SIDEBAR
# ====================
selected_cat = st.sidebar.selectbox("Category", CATEGORIES, key="ps_cat")
preset, ref_year, ref_month = period_selector(key="ps_period")
if ref_year is None:
    st.warning("No data loaded. Upload data or run scraper in Data Management.")
    st.stop()

FREQ_MAP = {"Monthly": "monthly", "Quarterly": "quarterly", "Financial Year": "annual"}
freq_label = st.sidebar.selectbox("Frequency", list(FREQ_MAP.keys()), key="ps_freq")
freq = FREQ_MAP[freq_label]

top_n = top_n_selector(key="ps_topn")

st.sidebar.divider()

_scrape_info = get_last_scrape_info()
if _scrape_info:
    with st.sidebar.expander("\U0001f504 Last Scraped", expanded=False):
        for si in _scrape_info:
            stype = si["scrape_type"].replace("national_", "N:").replace("state_", "S:")
            ts = si["last_completed"][:16] if si["last_completed"] else "Never"
            st.caption(f"**{stype}** \u2014 {ts}")

_import_stats = get_primary_import_stats()
if _import_stats:
    with st.sidebar.expander("\U0001f4e6 Primary Data Import", expanded=False):
        st.caption(f"**Total records:** {_import_stats.get('total_records', 0):,}")
        last_imp = _import_stats.get("last_import")
        if last_imp:
            st.caption(f"**Last import:** {str(last_imp)[:16]}")


# ====================
# LOAD DATA
# ====================
if not has_primary_data(selected_cat):
    st.info(f"No primary sales data available for {selected_cat}. Import data first.")
    st.stop()

cat_monthly = get_primary_category_monthly(selected_cat)
if cat_monthly.empty:
    st.info(f"No primary sales data for {selected_cat}.")
    st.stop()

cat_monthly["date"] = pd.to_datetime(
    cat_monthly["year"].astype(str) + "-" + cat_monthly["month"].astype(str).str.zfill(2) + "-01"
)

start_date, end_date = get_period_months(preset, ref_year, ref_month)
filtered_cat = filter_by_period(cat_monthly, start_date, end_date)
if filtered_cat.empty:
    st.info("No data for the selected period.")
    st.stop()

incomplete_periods = _get_incomplete_periods(filtered_cat, freq)


# ====================
# SECTION 1: KPI SNAPSHOT
# ====================
st.subheader(f"{selected_cat} Primary Sales \u2014 {format_month(ref_year, ref_month)}")

ref_data = cat_monthly[(cat_monthly["year"] == ref_year) & (cat_monthly["month"] == ref_month)]
prev_data = cat_monthly[(cat_monthly["year"] == ref_year - 1) & (cat_monthly["month"] == ref_month)]

ref_vol = ref_data["volume"].sum()
prev_vol = prev_data["volume"].sum()
yoy = round(((ref_vol / prev_vol) - 1) * 100, 1) if prev_vol > 0 else None

# FYTD calculation
fy_start_month = 4
if ref_month >= fy_start_month:
    fy_year = ref_year
else:
    fy_year = ref_year - 1
fytd_months = list(range(fy_start_month, 13)) + list(range(1, ref_month + 1)) if ref_month < fy_start_month else list(range(fy_start_month, ref_month + 1))

fytd_data = cat_monthly[
    ((cat_monthly["year"] == fy_year) & (cat_monthly["month"] >= fy_start_month)) |
    ((cat_monthly["year"] == fy_year + 1) & (cat_monthly["month"] <= ref_month) & (ref_month < fy_start_month))
]
fytd_vol = fytd_data["volume"].sum()

prev_fytd_data = cat_monthly[
    ((cat_monthly["year"] == fy_year - 1) & (cat_monthly["month"] >= fy_start_month)) |
    ((cat_monthly["year"] == fy_year) & (cat_monthly["month"] <= ref_month) & (ref_month < fy_start_month))
]
prev_fytd_vol = prev_fytd_data["volume"].sum()
fytd_yoy = round(((fytd_vol / prev_fytd_vol) - 1) * 100, 1) if prev_fytd_vol > 0 else None

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Volume", format_units(ref_vol))
with col2:
    delta = f"{yoy:+.1f}%" if yoy is not None else None
    st.metric("YoY Growth", delta if delta else "N/A")
with col3:
    st.metric("FYTD Volume", format_units(fytd_vol))
with col4:
    fytd_delta = f"{fytd_yoy:+.1f}%" if fytd_yoy is not None else None
    st.metric("FYTD YoY", fytd_delta if fytd_delta else "N/A")

st.divider()


# ====================
# SECTION 2: CATEGORY TREND (dual-axis)
# ====================
st.subheader(f"{selected_cat} Primary Sales Trend")

if freq == "monthly":
    cat_agg = filtered_cat.sort_values("date").copy()
    cat_agg["period_label"] = cat_agg.apply(
        lambda r: format_month(int(r["year"]), int(r["month"])), axis=1
    )
else:
    cat_agg = add_fy_columns(filtered_cat)
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

# Compute YoY from full dataset
full_cat = add_fy_columns(cat_monthly.copy())


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
        title=f"{selected_cat} Primary Sales & YoY Growth",
        bar_name="Volume", line_name="YoY %",
    )
    st.plotly_chart(fig, use_container_width=True)
else:
    import plotly.express as px

    fig = px.bar(cat_agg, x="label", y="volume", title=f"{selected_cat} Primary Sales")
    fig.update_layout(height=420)
    st.plotly_chart(fig, use_container_width=True)

st.divider()


# ====================
# SECTION 3: SEGMENT DATA TABLES
# ====================
st.subheader(f"Segment Data \u2014 {selected_cat}")

seg_monthly = get_primary_segment_monthly(selected_cat)
if not seg_monthly.empty:
    seg_monthly["date"] = pd.to_datetime(
        seg_monthly["year"].astype(str) + "-" + seg_monthly["month"].astype(str).str.zfill(2) + "-01"
    )
    filtered_seg = filter_by_period(seg_monthly, start_date, end_date)

    if not filtered_seg.empty:
        segments = filtered_seg["segment"].unique().tolist()

        seg_agg_frames = []
        for seg in segments:
            seg_slice = filtered_seg[filtered_seg["segment"] == seg].copy()
            if seg_slice.empty:
                continue
            agg = aggregate_by_frequency(seg_slice, freq)
            agg["segment"] = seg
            seg_agg_frames.append(agg)

        if seg_agg_frames:
            seg_tables = pd.concat(seg_agg_frames, ignore_index=True)
            seg_tables["label"] = seg_tables.apply(lambda r: _period_lbl(r, freq), axis=1)

            pivot_seg = seg_tables.pivot_table(
                index="segment", columns="label", values="volume", aggfunc="sum"
            )

            ordered_labels_seg = seg_tables.sort_values("date")["label"].unique().tolist()
            pivot_seg = pivot_seg.reindex(columns=ordered_labels_seg)

            total_row = pivot_seg.sum(axis=0)
            total_row.name = "TOTAL"
            pivot_seg = pd.concat([pivot_seg, total_row.to_frame().T])

            # Table A: Segment Volume
            st.markdown("**Segment Volume (units)**")
            seg_vol_display = pivot_seg.copy()
            for col in seg_vol_display.columns:
                seg_vol_display[col] = seg_vol_display[col].apply(
                    lambda v: f"{int(v):,}" if pd.notna(v) and v > 0 else "\u2014"
                )
            seg_vol_display.index.name = "Segment"
            st.dataframe(seg_vol_display, use_container_width=True)

            # Table B: Segment YoY Growth %
            with st.expander("Segment YoY Growth %"):
                seg_yoy_data = pivot_seg.copy().astype(float)
                seg_row_labels = [s for s in segments if s in seg_yoy_data.index] + ["TOTAL"]
                seg_yoy_result = pd.DataFrame(index=seg_row_labels, columns=ordered_labels_seg, dtype=object)

                for seg in seg_row_labels:
                    for col_idx, col in enumerate(ordered_labels_seg):
                        if col in incomplete_periods:
                            seg_yoy_result.loc[seg, col] = "\u2014"
                            continue

                        curr = (
                            seg_yoy_data.loc[seg, col]
                            if seg in seg_yoy_data.index and col in seg_yoy_data.columns
                            else None
                        )

                        if freq == "monthly" and col_idx >= 12:
                            prev_label = ordered_labels_seg[col_idx - 12]
                        elif freq == "quarterly" and col_idx >= 4:
                            prev_label = ordered_labels_seg[col_idx - 4]
                        elif freq == "annual" and col_idx >= 1:
                            prev_label = ordered_labels_seg[col_idx - 1]
                        else:
                            prev_label = None

                        if prev_label and prev_label in incomplete_periods:
                            seg_yoy_result.loc[seg, col] = "\u2014"
                            continue

                        if prev_label and prev_label in seg_yoy_data.columns:
                            prev = seg_yoy_data.loc[seg, prev_label]
                            if pd.notna(curr) and pd.notna(prev) and prev > 0:
                                yoy_val = round(((curr / prev) - 1) * 100, 1)
                                seg_yoy_result.loc[seg, col] = f"{yoy_val:+.1f}%"
                            else:
                                seg_yoy_result.loc[seg, col] = "\u2014"
                        else:
                            seg_yoy_result.loc[seg, col] = "\u2014"

                seg_yoy_result.index.name = "Segment"
                st.dataframe(seg_yoy_result, use_container_width=True)

            # Table C: Segment Mix %
            with st.expander("Segment Mix %"):
                seg_no_total = pivot_seg.drop("TOTAL", errors="ignore")
                seg_totals = seg_no_total.sum(axis=0)
                seg_mix_display = seg_no_total.copy()

                for col in seg_mix_display.columns:
                    total = seg_totals[col]
                    if pd.notna(total) and total > 0:
                        seg_mix_display[col] = seg_mix_display[col].apply(
                            lambda v, t=total: f"{v / t * 100:.1f}%" if pd.notna(v) else "\u2014"
                        )
                    else:
                        seg_mix_display[col] = "\u2014"

                seg_mix_display.index.name = "Segment"
                st.dataframe(seg_mix_display, use_container_width=True)
    else:
        st.info("No segment data for the selected period.")
else:
    st.info("No segment data available for this category.")

st.divider()


# ====================
# SECTION 4: OEM DATA TABLES
# ====================
st.subheader(f"OEM Data \u2014 {selected_cat}")

oem_monthly = get_primary_oem_monthly(selected_cat)
if oem_monthly.empty:
    st.info(f"No OEM-level primary sales data for {selected_cat}.")
    st.stop()

oem_monthly["date"] = pd.to_datetime(
    oem_monthly["year"].astype(str) + "-" + oem_monthly["month"].astype(str).str.zfill(2) + "-01"
)
filtered_oem = filter_by_period(oem_monthly, start_date, end_date)
if filtered_oem.empty:
    st.info("No OEM data for the selected period.")
    st.stop()

# Determine top N OEMs by reference month volume
ref_oem = oem_monthly[
    (oem_monthly["year"] == ref_year) & (oem_monthly["month"] == ref_month)
].copy()
if ref_oem.empty:
    ref_oem = filtered_oem.groupby("oem_name").agg(volume=("volume", "sum")).reset_index()

top_oems = ref_oem.nlargest(top_n, "volume")["oem_name"].tolist()

# Aggregate per OEM
agg_frames = []
for oem_name in top_oems:
    oem_slice = filtered_oem[filtered_oem["oem_name"] == oem_name].copy()
    if oem_slice.empty:
        continue
    agg = aggregate_by_frequency(oem_slice, freq)
    agg["oem_name"] = oem_name
    agg_frames.append(agg)

# Others
others = filtered_oem[~filtered_oem["oem_name"].isin(top_oems)].copy()
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

# Table A: OEM Volume
st.markdown("**OEM Volume (units)**")
vol_display = pivot_vol.copy()
for col in vol_display.columns:
    vol_display[col] = vol_display[col].apply(
        lambda v: f"{int(v):,}" if pd.notna(v) and v > 0 else "\u2014"
    )
vol_display.index.name = "OEM"
st.dataframe(vol_display, use_container_width=True)

# Table B: OEM YoY Growth %
with st.expander("OEM YoY Growth %"):
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

# Table C: OEM Market Share %
with st.expander("OEM Market Share %"):
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


# ====================
# SECTION 5: OEM MARKET SHARE TREND (line chart)
# ====================
st.subheader(f"OEM Market Share Trend \u2014 {selected_cat}")

share_frames = []
for oem_name in top_oems[:7]:  # Limit to 7 for readability
    oem_slice = filtered_oem[filtered_oem["oem_name"] == oem_name].copy()
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
        title=f"{selected_cat} Primary Sales \u2014 OEM Market Share (%)",
        yaxis_title="Share %",
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
    )
    st.plotly_chart(fig_share, use_container_width=True)
