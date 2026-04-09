"""Page 12: Primary OEM 360 - Deep-dive into a single OEM's primary (wholesale) sales.

Tables only -- no charts.  Mirrors the table formatting patterns from State Performance
and Primary Sales pages (right-aligned numbers, full comma formatting).
"""
import streamlit as st
import pandas as pd
import numpy as np
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_primary_oem_total, get_primary_oem_segments,
    get_primary_category_monthly, get_primary_segment_monthly,
    get_primary_oems_list, get_last_scrape_info,
    has_primary_data,
)
from config.primary_sales_config import (
    get_segment_order, get_super_segments, get_super_segment_order,
)
from components.filters import primary_period_selector
from components.formatters import format_units, format_month, get_fy_label
from components.analysis import (
    aggregate_by_frequency, filter_by_period, get_period_months, add_fy_columns,
)

init_db()

# -- Right-align numbers in dataframes via CSS --
st.markdown("""
<style>
div[data-testid="stDataFrame"] td { text-align: right !important; }
div[data-testid="stDataFrame"] th { text-align: center !important; }
div[data-testid="stDataFrame"] table { font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)

st.title("Primary OEM 360")

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


def _fy_label_with_ytd(fy_start, data):
    """Return 'FY26' or 'YTDFY26' — only the latest FY gets YTD label.

    Historical FYs with <12 months (e.g., FY21 missing Apr 2020 due to
    COVID zero sales) should NOT be labeled YTD.
    """
    label = get_fy_label(fy_start)
    max_fy = int(data["fy"].max())
    # Only mark the latest FY as YTD if incomplete
    if fy_start == max_fy:
        fy_months = data[data["fy"] == fy_start]
        if len(fy_months["month"].unique()) < 12:
            return f"YTD{label}"
    return label


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


def _apply_ytd_labels(agg_df, raw_data, freq):
    """Replace FY labels with YTDFY labels for incomplete fiscal years."""
    if freq != "annual":
        return agg_df
    df_fy = add_fy_columns(raw_data)
    agg_df = agg_df.copy()
    for idx, row in agg_df.iterrows():
        if "fy" in row.index:
            new_label = _fy_label_with_ytd(int(row["fy"]), df_fy)
            agg_df.at[idx, "period_label"] = new_label
            agg_df.at[idx, "fy_label"] = new_label
    return agg_df


def _compute_yoy_row(pivot_numeric, row_name, ordered_labels, freq, incomplete):
    """Return a dict {label: formatted_yoy} for a single row in a pivot table."""
    result = {}
    for col_idx, col in enumerate(ordered_labels):
        curr = pivot_numeric.loc[row_name, col] if col in pivot_numeric.columns else np.nan

        if freq == "monthly":
            prev_offset = 12
        elif freq == "quarterly":
            prev_offset = 4
        else:
            prev_offset = 1

        prev_label = ordered_labels[col_idx - prev_offset] if col_idx >= prev_offset else None
        if prev_label and prev_label in pivot_numeric.columns:
            prev = pivot_numeric.loc[row_name, prev_label]
            if pd.notna(curr) and pd.notna(prev) and prev > 0:
                yoy_val = round(((curr / prev) - 1) * 100, 1)
                result[col] = _fmt_growth(yoy_val)
            else:
                result[col] = "\u2014"
        else:
            result[col] = "\u2014"
    return result


# ====================
# SIDEBAR
# ====================
cat_kwargs = {"key": "pri_cat"}
if "pri_cat" not in st.session_state:
    cat_kwargs["index"] = CATEGORIES.index("2W") if "2W" in CATEGORIES else 0
selected_cat = st.sidebar.selectbox("Category", CATEGORIES, **cat_kwargs)

if not has_primary_data(selected_cat):
    st.info(f"No primary sales data available for {selected_cat}. Import data first.")
    st.stop()

oem_list = get_primary_oems_list(selected_cat)
if not oem_list:
    st.info(f"No OEMs found for {selected_cat}.")
    st.stop()

oem_kwargs = {"key": "poem_oem"}
if "poem_oem" not in st.session_state and "Hero MotoCorp" in oem_list:
    oem_kwargs["index"] = oem_list.index("Hero MotoCorp")
selected_oem = st.sidebar.selectbox("OEM", oem_list, **oem_kwargs)

preset, ref_year, ref_month = primary_period_selector(key="pri_period")
if ref_year is None:
    st.warning("No data loaded. Upload data or run scraper in Data Management.")
    st.stop()

FREQ_MAP = {"Monthly": "monthly", "Quarterly": "quarterly", "Financial Year": "annual"}
freq_label = st.sidebar.selectbox("Frequency", list(FREQ_MAP.keys()), key="pri_freq")
freq = FREQ_MAP[freq_label]

st.sidebar.divider()
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
oem_total_raw = get_primary_oem_total(selected_oem, selected_cat)
if oem_total_raw.empty:
    st.info(f"No data for {selected_oem} in {selected_cat}.")
    st.stop()

oem_total_raw["date"] = pd.to_datetime(
    oem_total_raw["year"].astype(str) + "-"
    + oem_total_raw["month"].astype(str).str.zfill(2) + "-01"
)

start_date, end_date = get_period_months(preset, ref_year, ref_month)
oem_filtered = filter_by_period(oem_total_raw, start_date, end_date)
if oem_filtered.empty:
    st.info("No data for the selected period.")
    st.stop()

incomplete_periods = _get_incomplete_periods(oem_filtered, freq)

# Aggregate OEM total by frequency
oem_agg = aggregate_by_frequency(oem_filtered.copy(), freq)

# Apply YTD labels
if freq == "annual":
    oem_agg = _apply_ytd_labels(oem_agg, oem_filtered, freq)

oem_agg["label"] = oem_agg.apply(lambda r: _period_lbl(r, freq), axis=1)
ordered_labels = oem_agg.sort_values("date")["label"].unique().tolist()


# Also load category total for market share
cat_monthly = get_primary_category_monthly(selected_cat)
cat_monthly["date"] = pd.to_datetime(
    cat_monthly["year"].astype(str) + "-"
    + cat_monthly["month"].astype(str).str.zfill(2) + "-01"
)
cat_filtered = filter_by_period(cat_monthly, start_date, end_date)
cat_agg = aggregate_by_frequency(cat_filtered.copy(), freq)
if freq == "annual":
    cat_agg = _apply_ytd_labels(cat_agg, cat_filtered, freq)
cat_agg["label"] = cat_agg.apply(lambda r: _period_lbl(r, freq), axis=1)

# Segment data
oem_seg_raw = get_primary_oem_segments(selected_oem, selected_cat)
seg_total_raw = get_primary_segment_monthly(selected_cat)
has_seg_data = not oem_seg_raw.empty

if has_seg_data:
    oem_seg_raw["date"] = pd.to_datetime(
        oem_seg_raw["year"].astype(str) + "-"
        + oem_seg_raw["month"].astype(str).str.zfill(2) + "-01"
    )
    oem_seg_filtered = filter_by_period(oem_seg_raw, start_date, end_date)
    oem_segments = oem_seg_filtered["segment"].unique().tolist()

    seg_total_raw["date"] = pd.to_datetime(
        seg_total_raw["year"].astype(str) + "-"
        + seg_total_raw["month"].astype(str).str.zfill(2) + "-01"
    )
    seg_total_filtered = filter_by_period(seg_total_raw, start_date, end_date)


# ====================
# SECTION 1: KPI SNAPSHOT
# ====================
st.subheader(f"{selected_oem} \u2014 {selected_cat} Primary Sales")

ref_data = oem_total_raw[
    (oem_total_raw["year"] == ref_year) & (oem_total_raw["month"] == ref_month)
]
prev_data = oem_total_raw[
    (oem_total_raw["year"] == ref_year - 1) & (oem_total_raw["month"] == ref_month)
]
ref_vol = ref_data["volume"].sum()
prev_vol = prev_data["volume"].sum()
yoy = round(((ref_vol / prev_vol) - 1) * 100, 1) if prev_vol > 0 else None

# FYTD
fy_start_month = 4
fy_year = ref_year if ref_month >= fy_start_month else ref_year - 1
fytd_data = oem_total_raw[
    ((oem_total_raw["year"] == fy_year) & (oem_total_raw["month"] >= fy_start_month))
    | (
        (oem_total_raw["year"] == fy_year + 1)
        & (oem_total_raw["month"] <= ref_month)
        & (ref_month < fy_start_month)
    )
]
fytd_vol = fytd_data["volume"].sum()

prev_fytd_data = oem_total_raw[
    ((oem_total_raw["year"] == fy_year - 1) & (oem_total_raw["month"] >= fy_start_month))
    | (
        (oem_total_raw["year"] == fy_year)
        & (oem_total_raw["month"] <= ref_month)
        & (ref_month < fy_start_month)
    )
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
# SECTION 2: OEM VOLUME + YoY TREND TABLE (combined, no expander)
# ====================
st.subheader("Volume & YoY Trend")

# Combined Volume + YoY in one table
vol_dict = {}
for _, r in oem_agg.iterrows():
    vol_dict[r["label"]] = r["volume"]

oem_pivot = pd.DataFrame(
    [{col: vol_dict.get(col, np.nan) for col in ordered_labels}],
    index=["Volume"],
)
yoy_vals = _compute_yoy_row(oem_pivot.astype(float), "Volume", ordered_labels, freq, incomplete_periods)

combined_trend = pd.DataFrame([
    {col: _fmt_vol(vol_dict.get(col)) for col in ordered_labels},
    yoy_vals,
], index=["Volume", "YoY %"])
combined_trend.index.name = ""
st.dataframe(combined_trend.iloc[:, ::-1], width="stretch")

st.divider()


# ====================
# SECTION 3: OVERALL MARKET SHARE TABLE
# ====================
st.subheader("Overall Market Share")

cat_vol_dict = {}
for _, r in cat_agg.iterrows():
    cat_vol_dict[r["label"]] = r["volume"]

share_dict = {}
for col in ordered_labels:
    ov = vol_dict.get(col, 0) or 0
    cv = cat_vol_dict.get(col, 0) or 0
    if cv > 0:
        share_dict[col] = f"{ov / cv * 100:.1f}%"
    else:
        share_dict[col] = "\u2014"

share_row = pd.DataFrame([share_dict], index=["Market Share %"])
share_row.index.name = ""
st.dataframe(share_row, width="stretch")

# -- Overall Market Share Line Chart --
st.markdown("**Overall Market Share Trend**")

import plotly.graph_objects as go

# Build share data: OEM vol / category total per period
share_trend_data = []
for col in ordered_labels:
    oem_v = vol_dict.get(col, 0)
    cat_v = cat_vol_dict.get(col, 0)
    if cat_v > 0:
        share_trend_data.append({
            "label": col,
            "share_pct": round(oem_v / cat_v * 100, 1),
        })

if share_trend_data:
    std_df = pd.DataFrame(share_trend_data)
    fig_overall = go.Figure()
    fig_overall.add_trace(go.Scatter(
        x=std_df["label"], y=std_df["share_pct"],
        name=selected_oem, mode="lines+markers",
        line=dict(width=2.5, color="#1f77b4"),
        marker=dict(size=6),
        hovertemplate="%{y:.1f}%<extra>" + selected_oem + "</extra>",
    ))
    fig_overall.update_layout(
        height=380,
        title=f"{selected_oem} — Overall {selected_cat} Market Share",
        yaxis=dict(title="Share %", ticksuffix="%"),
        hovermode="x unified",
        margin=dict(l=40, r=20, t=50, b=60),
    )
    fig_overall.update_xaxes(title="")
    st.plotly_chart(fig_overall, width="stretch")

st.divider()


# ====================
# SECTION 4: SALES MIX TABLE (sub-segment breakdown with super-segment subtotals)
# ====================
if has_seg_data and not oem_seg_filtered.empty:
    st.subheader("Sales Mix (Sub-Segment Breakdown)")

    # Aggregate each segment
    seg_agg_frames = []
    for seg in oem_segments:
        seg_slice = oem_seg_filtered[oem_seg_filtered["segment"] == seg].copy()
        if seg_slice.empty:
            continue
        agg = aggregate_by_frequency(seg_slice, freq)
        if freq == "annual":
            agg = _apply_ytd_labels(agg, seg_slice, freq)
        agg["segment"] = seg
        seg_agg_frames.append(agg)

    if seg_agg_frames:
        seg_tables = pd.concat(seg_agg_frames, ignore_index=True)
        seg_tables["label"] = seg_tables.apply(lambda r: _period_lbl(r, freq), axis=1)

        pivot_seg = seg_tables.pivot_table(
            index="segment", columns="label", values="volume", aggfunc="sum"
        ).reindex(columns=ordered_labels)

        # Build ordered rows with super-segment subtotals and indented formatting
        segment_order = get_segment_order(selected_cat)
        super_segments = get_super_segments(selected_cat)
        super_order = get_super_segment_order(selected_cat)

        display_rows = []  # list of (row_name, Series, is_subtotal)

        for ss_name in super_order:
            ss_children = super_segments.get(ss_name, [])
            present_children = [s for s in ss_children if s in pivot_seg.index]
            if not present_children:
                continue

            for seg in present_children:
                display_rows.append((f"  {seg}", pivot_seg.loc[seg], False))  # 2-space indent

            # Always add subtotal for super-segments
            if present_children:
                subtotal = pivot_seg.loc[present_children].sum(axis=0)
                display_rows.append((ss_name, subtotal, True))  # No indent for subtotals

        # Add segments not in any super-segment (indented)
        known_segs = set()
        for children in super_segments.values():
            known_segs.update(children)
        orphan_segs = [s for s in pivot_seg.index if s not in known_segs]
        for seg in orphan_segs:
            display_rows.append((f"  {seg}", pivot_seg.loc[seg], False))

        # Grand TOTAL row
        total_series = pivot_seg.sum(axis=0)
        grand_total_label = f"Total {selected_cat}"
        display_rows.append((grand_total_label, total_series, True))

        # Build display DataFrame
        row_names = [r[0] for r in display_rows]
        mix_data = pd.DataFrame(
            [r[1].values for r in display_rows],
            index=row_names,
            columns=ordered_labels,
        )

        # Table A: Volume
        st.markdown("**Segment Volume (units)**")
        vol_display = mix_data.copy()
        for col in vol_display.columns:
            vol_display[col] = vol_display[col].apply(
                lambda v: f"{int(v):,}" if pd.notna(v) and v > 0 else "\u2014"
            )
        vol_display.index.name = "Segment"
        st.dataframe(vol_display.iloc[:, ::-1], width="stretch")

        # Table B: Mix %
        with st.expander("Segment Mix % (of OEM total)"):
            total_vals = mix_data.loc[grand_total_label].astype(float)
            mix_pct = mix_data.drop(
                [n for n, _, is_sub in display_rows if is_sub], errors="ignore"
            ).copy()
            for col in mix_pct.columns:
                t = total_vals[col]
                if pd.notna(t) and t > 0:
                    mix_pct[col] = mix_pct[col].apply(
                        lambda v, tot=t: f"{v / tot * 100:.1f}%" if pd.notna(v) and v > 0 else "\u2014"
                    )
                else:
                    mix_pct[col] = "\u2014"
            mix_pct.index.name = "Segment"
            st.dataframe(mix_pct.iloc[:, ::-1], width="stretch")

    st.divider()


    # ====================
    # SECTION 5: SUB-SEGMENT MARKET SHARE TABLE
    # ====================
    st.subheader("Sub-Segment Market Share")

    # Aggregate category-level segment totals
    seg_tot_agg_frames = []
    for seg in oem_segments:
        seg_slice = seg_total_filtered[seg_total_filtered["segment"] == seg].copy()
        if seg_slice.empty:
            continue
        agg = aggregate_by_frequency(seg_slice, freq)
        if freq == "annual":
            agg = _apply_ytd_labels(agg, seg_slice, freq)
        agg["segment"] = seg
        seg_tot_agg_frames.append(agg)

    if seg_tot_agg_frames:
        seg_tot_tables = pd.concat(seg_tot_agg_frames, ignore_index=True)
        seg_tot_tables["label"] = seg_tot_tables.apply(lambda r: _period_lbl(r, freq), axis=1)

        pivot_seg_total = seg_tot_tables.pivot_table(
            index="segment", columns="label", values="volume", aggfunc="sum"
        ).reindex(columns=ordered_labels)

        # Compute share = OEM segment vol / category segment vol
        share_rows = {}
        seg_share_numeric = {}  # parallel numeric version for charting
        # Use config order, only segments OEM participates in
        ordered_segs = [s for s in get_segment_order(selected_cat) if s in pivot_seg.index]
        remaining = [s for s in pivot_seg.index if s not in ordered_segs]
        ordered_segs += remaining

        for seg in ordered_segs:
            row_vals = {}
            num_vals = {}
            for col in ordered_labels:
                oem_v = pivot_seg.loc[seg, col] if col in pivot_seg.columns else 0
                tot_v = (
                    pivot_seg_total.loc[seg, col]
                    if seg in pivot_seg_total.index and col in pivot_seg_total.columns
                    else 0
                )
                if pd.notna(oem_v) and pd.notna(tot_v) and tot_v > 0:
                    pct = oem_v / tot_v * 100
                    row_vals[col] = f"{pct:.1f}%"
                    num_vals[col] = pct
                else:
                    row_vals[col] = "\u2014"
                    num_vals[col] = np.nan
            share_rows[seg] = row_vals
            seg_share_numeric[seg] = num_vals

        share_df = pd.DataFrame(share_rows).T.reindex(columns=ordered_labels)
        share_df.index.name = "Segment"
        st.dataframe(share_df.iloc[:, ::-1], width="stretch")

        # Numeric pivot for line chart
        seg_share_pivot = pd.DataFrame(seg_share_numeric).T.reindex(columns=ordered_labels)

        # -- Sub-Segment Market Share Line Chart --
        st.markdown("**Sub-Segment Market Share Trend**")

        from components.charts import market_share_line_chart

        # Build long-form from seg_share_pivot (OEM's share in each sub-segment over time)
        sub_share_long = []
        for seg_name in seg_share_pivot.index:
            for col in ordered_labels:
                if col in seg_share_pivot.columns:
                    v = seg_share_pivot.loc[seg_name, col]
                    if pd.notna(v):
                        sub_share_long.append({
                            "oem_name": seg_name.strip(),  # Remove any indent
                            "label": col,
                            "share_pct": round(float(v), 1),
                            "date_sort": ordered_labels.index(col),
                        })

        if sub_share_long:
            ssl_df = pd.DataFrame(sub_share_long).sort_values("date_sort")
            fig_subshare = market_share_line_chart(
                ssl_df, title=f"{selected_oem} — Sub-Segment Market Share Trend",
                date_col="date_sort",
            )
            st.plotly_chart(fig_subshare, width="stretch")

    st.divider()


    # ====================
    # SECTION 6: SUB-SEGMENT YoY GROWTH TABLE
    # ====================
    st.subheader("Sub-Segment YoY Growth")

    if seg_agg_frames:
        pivot_seg_numeric = pivot_seg.astype(float)

        growth_rows = {}
        for seg in ordered_segs:
            if seg not in pivot_seg_numeric.index:
                continue
            growth_rows[seg] = _compute_yoy_row(
                pivot_seg_numeric, seg, ordered_labels, freq, incomplete_periods
            )

        if growth_rows:
            growth_df = pd.DataFrame(growth_rows).T.reindex(columns=ordered_labels)
            growth_df.index.name = "Segment"
            st.dataframe(growth_df.iloc[:, ::-1], width="stretch")
        else:
            st.info("Not enough data to compute YoY growth.")

else:
    st.info(f"No sub-segment data available for {selected_oem} in {selected_cat}.")
