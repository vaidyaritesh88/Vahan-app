"""Page 9: Primary Sales - Wholesale/dispatch sales analysis with PC/UV grouping.

All analysis via TABLES except the category trend dual-axis bar+line chart.
Segment tables include super-segment subtotals (PC/UV for PV, Motorcycle etc for 2W).
"""
import streamlit as st
import pandas as pd
import numpy as np
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_primary_category_monthly, get_primary_oem_monthly,
    get_primary_segment_monthly, get_primary_import_stats,
    has_primary_data, get_last_scrape_info,
)
from config.primary_sales_config import (
    get_segment_order, get_super_segments, get_super_segment_order,
)
from components.filters import period_selector, top_n_selector
from components.formatters import format_units, format_month
from components.charts import dual_axis_bar_line
from components.analysis import (
    aggregate_by_frequency, filter_by_period, get_period_months, add_fy_columns,
)

init_db()

st.set_page_config(page_title="Primary Sales", page_icon="\U0001f4e6", layout="wide")

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


def _compute_yoy_table(pivot_vol, row_labels, ordered_labels, freq, incomplete_periods):
    """Build a YoY growth % DataFrame from a volume pivot table."""
    data = pivot_vol.copy().astype(float)
    result = pd.DataFrame(index=row_labels, columns=ordered_labels, dtype=object)

    for label in row_labels:
        for col_idx, col in enumerate(ordered_labels):
            if col in incomplete_periods:
                result.loc[label, col] = "\u2014"
                continue

            curr = data.loc[label, col] if label in data.index and col in data.columns else None

            if freq == "monthly" and col_idx >= 12:
                prev_label = ordered_labels[col_idx - 12]
            elif freq == "quarterly" and col_idx >= 4:
                prev_label = ordered_labels[col_idx - 4]
            elif freq == "annual" and col_idx >= 1:
                prev_label = ordered_labels[col_idx - 1]
            else:
                prev_label = None

            if prev_label and prev_label in incomplete_periods:
                result.loc[label, col] = "\u2014"
                continue

            if prev_label and prev_label in data.columns:
                prev = data.loc[label, prev_label] if label in data.index else None
                if pd.notna(curr) and pd.notna(prev) and prev > 0:
                    yoy_val = round(((curr / prev) - 1) * 100, 1)
                    result.loc[label, col] = _fmt_growth(yoy_val)
                else:
                    result.loc[label, col] = "\u2014"
            else:
                result.loc[label, col] = "\u2014"
    return result


def _build_segment_pivot(filtered_seg, freq, category, ordered_labels):
    """Build segment volume pivot with super-segment subtotals in correct order.

    Returns (pivot_df, row_order) where row_order includes segments,
    subtotals, and grand total in the specified display order.
    """
    seg_order = get_segment_order(category)
    super_segs = get_super_segments(category)
    super_order = get_super_segment_order(category)

    # Aggregate each segment
    seg_agg_frames = []
    for seg in seg_order:
        seg_slice = filtered_seg[filtered_seg["segment"] == seg].copy()
        if seg_slice.empty:
            continue
        agg = aggregate_by_frequency(seg_slice, freq)
        agg["segment"] = seg
        seg_agg_frames.append(agg)

    if not seg_agg_frames:
        return None, None, None

    seg_tables = pd.concat(seg_agg_frames, ignore_index=True)
    seg_tables["label"] = seg_tables.apply(lambda r: _period_lbl(r, freq), axis=1)

    pivot = seg_tables.pivot_table(
        index="segment", columns="label", values="volume", aggfunc="sum"
    )
    pivot = pivot.reindex(columns=ordered_labels).fillna(0)

    # Compute subtotals for each super-segment
    subtotals = {}
    for ss_name in super_order:
        members = [s for s in super_segs[ss_name] if s in pivot.index]
        if members:
            subtotals[ss_name] = pivot.loc[members].sum(axis=0)

    grand_total = pivot.sum(axis=0)
    grand_total_label = f"{category} TOTAL"

    # Assemble rows in correct order
    rows = []
    row_order = []
    for ss_name in super_order:
        members = [s for s in super_segs[ss_name] if s in pivot.index]
        for seg in members:
            rows.append(pivot.loc[seg])
            row_order.append(seg)
        if ss_name in subtotals:
            rows.append(subtotals[ss_name])
            row_order.append(ss_name)

    rows.append(grand_total)
    row_order.append(grand_total_label)

    final = pd.DataFrame(rows, index=row_order, columns=ordered_labels)

    # Identify bold rows (subtotals + grand total)
    bold_rows = set(subtotals.keys()) | {grand_total_label}

    return final, row_order, bold_rows


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

_import_stats = get_primary_import_stats()
if _import_stats:
    with st.sidebar.expander("\U0001f4e6 Primary Data Import", expanded=False):
        st.caption(f"**Records:** {_import_stats.get('total_records', 0):,}")
        st.caption(f"**OEMs:** {_import_stats.get('oem_count', 0)}")
        st.caption(f"**Range:** {_import_stats.get('first_month', '')} to {_import_stats.get('last_month', '')}")
        last_imp = _import_stats.get("last_import")
        if last_imp:
            st.caption(f"**Last import:** {str(last_imp)[:16]}")

_scrape_info = get_last_scrape_info()
if _scrape_info:
    with st.sidebar.expander("\U0001f504 Last Scraped", expanded=False):
        for si in _scrape_info:
            stype = si["scrape_type"].replace("national_", "N:").replace("state_", "S:")
            ts = si["last_completed"][:16] if si["last_completed"] else "Never"
            st.caption(f"**{stype}** \u2014 {ts}")


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
    st.metric("YoY Growth", f"{yoy:+.1f}%" if yoy is not None else "N/A")
with col3:
    st.metric("FYTD Volume", format_units(fytd_vol))
with col4:
    st.metric("FYTD YoY", f"{fytd_yoy:+.1f}%" if fytd_yoy is not None else "N/A")

st.divider()


# ====================
# SECTION 2: CATEGORY TREND (dual-axis bar+line)
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
            ), axis=1,
        )
        cat_agg["period_label"] = cat_agg["q_label"]
    else:
        cat_agg = cat_agg.groupby(["fy", "fy_label"]).agg(
            volume=("volume", "sum")
        ).reset_index()
        cat_agg["date"] = cat_agg["fy"].apply(lambda y: pd.Timestamp(int(y), 10, 1))
        cat_agg["period_label"] = cat_agg["fy_label"]

cat_agg = cat_agg.sort_values("date")

# YoY from full dataset
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
    return round(((row["volume"] / pv) - 1) * 100, 1) if pv > 0 else None


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

# Common ordered labels from cat_agg for all tables
ordered_labels = cat_agg.sort_values("date")["label"].unique().tolist()


# ====================
# SECTION 3: SEGMENT VOLUME TABLE (with subtotals)
# ====================
st.subheader(f"Segment Data \u2014 {selected_cat}")

seg_monthly = get_primary_segment_monthly(selected_cat)
seg_pivot = None

if not seg_monthly.empty:
    seg_monthly["date"] = pd.to_datetime(
        seg_monthly["year"].astype(str) + "-" + seg_monthly["month"].astype(str).str.zfill(2) + "-01"
    )
    filtered_seg = filter_by_period(seg_monthly, start_date, end_date)

    if not filtered_seg.empty:
        result = _build_segment_pivot(filtered_seg, freq, selected_cat, ordered_labels)
        if result[0] is not None:
            seg_pivot, seg_row_order, bold_rows = result

            # Table: Segment Volume
            st.markdown("**Segment Volume (units)**")
            vol_disp = seg_pivot.copy()
            for col in vol_disp.columns:
                vol_disp[col] = vol_disp[col].apply(_fmt_vol)
            vol_disp.index.name = "Segment"
            st.dataframe(vol_disp, use_container_width=True)

            # Section 4: Segment YoY Growth (expander)
            with st.expander("Segment YoY Growth %"):
                seg_yoy = _compute_yoy_table(
                    seg_pivot, seg_row_order, ordered_labels, freq, incomplete_periods
                )
                seg_yoy.index.name = "Segment"
                st.dataframe(seg_yoy, use_container_width=True)

            # Section 5: Segment Mix % (expander)
            with st.expander("Segment Mix %"):
                grand_label = f"{selected_cat} TOTAL"
                totals_row = seg_pivot.loc[grand_label] if grand_label in seg_pivot.index else seg_pivot.sum(axis=0)
                mix_disp = seg_pivot.copy()
                # Only show individual segments (not subtotals/grand total)
                for col in mix_disp.columns:
                    total = totals_row[col]
                    if pd.notna(total) and total > 0:
                        mix_disp[col] = mix_disp[col].apply(
                            lambda v, t=total: f"{v / t * 100:.1f}%" if pd.notna(v) and v > 0 else "\u2014"
                        )
                    else:
                        mix_disp[col] = "\u2014"
                mix_disp.index.name = "Segment"
                st.dataframe(mix_disp, use_container_width=True)
        else:
            st.info("No segment data for the selected period.")
    else:
        st.info("No segment data for the selected period.")
else:
    st.info("No segment data available for this category.")

st.divider()


# ====================
# SECTION 6: OEM VOLUME TABLE
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

# Top N OEMs by reference month volume
ref_oem = oem_monthly[
    (oem_monthly["year"] == ref_year) & (oem_monthly["month"] == ref_month)
].copy()
if ref_oem.empty:
    ref_oem = filtered_oem.groupby("oem_name").agg(volume=("volume", "sum")).reset_index()

top_oems = ref_oem.nlargest(top_n, "volume")["oem_name"].tolist()

# Aggregate per OEM
oem_agg_frames = []
for oem_name in top_oems:
    oem_slice = filtered_oem[filtered_oem["oem_name"] == oem_name].copy()
    if oem_slice.empty:
        continue
    agg = aggregate_by_frequency(oem_slice, freq)
    agg["oem_name"] = oem_name
    oem_agg_frames.append(agg)

# Others
others = filtered_oem[~filtered_oem["oem_name"].isin(top_oems)].copy()
if not others.empty:
    others_agg = aggregate_by_frequency(others, freq)
    others_agg["oem_name"] = "Others"
    oem_agg_frames.append(others_agg)

if not oem_agg_frames:
    st.info("No OEM data for tables.")
    st.stop()

oem_tables = pd.concat(oem_agg_frames, ignore_index=True)
oem_tables["label"] = oem_tables.apply(lambda r: _period_lbl(r, freq), axis=1)

oem_pivot = oem_tables.pivot_table(
    index="oem_name", columns="label", values="volume", aggfunc="sum"
)
oem_pivot = oem_pivot.reindex(columns=ordered_labels).fillna(0)

oem_order = [o for o in top_oems if o in oem_pivot.index]
if "Others" in oem_pivot.index:
    oem_order.append("Others")
oem_pivot = oem_pivot.reindex(oem_order)

total_row = oem_pivot.sum(axis=0)
total_row.name = "TOTAL"
oem_pivot = pd.concat([oem_pivot, total_row.to_frame().T])

oem_row_labels = oem_order + ["TOTAL"]

# Table: OEM Volume
st.markdown("**OEM Volume (units)**")
oem_vol_disp = oem_pivot.copy()
for col in oem_vol_disp.columns:
    oem_vol_disp[col] = oem_vol_disp[col].apply(_fmt_vol)
oem_vol_disp.index.name = "OEM"
st.dataframe(oem_vol_disp, use_container_width=True)

# Section 7: OEM YoY Growth (expander)
with st.expander("OEM YoY Growth %"):
    oem_yoy = _compute_yoy_table(
        oem_pivot, oem_row_labels, ordered_labels, freq, incomplete_periods
    )
    oem_yoy.index.name = "OEM"
    st.dataframe(oem_yoy, use_container_width=True)

# Section 8: OEM Market Share (expander)
with st.expander("OEM Market Share %"):
    share_disp = oem_pivot.drop("TOTAL", errors="ignore").copy()
    totals = oem_pivot.loc["TOTAL"] if "TOTAL" in oem_pivot.index else oem_pivot.sum(axis=0)

    for col in share_disp.columns:
        total = totals[col]
        if pd.notna(total) and total > 0:
            share_disp[col] = share_disp[col].apply(
                lambda v, t=total: f"{v / t * 100:.1f}%" if pd.notna(v) and v > 0 else "\u2014"
            )
        else:
            share_disp[col] = "\u2014"

    share_disp.index.name = "OEM"
    st.dataframe(share_disp, use_container_width=True)
