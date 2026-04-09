"""Page 11: Primary Sub-Segment Analysis - Deep dive into a specific primary sales sub-segment."""
import streamlit as st
import pandas as pd
import numpy as np
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_primary_model_monthly, get_primary_oem_in_segment,
    get_primary_segment_monthly, get_primary_category_monthly,
    get_primary_segments_list, get_last_scrape_info,
    has_primary_data,
)
from config.primary_sales_config import get_segment_order
from components.filters import primary_period_selector, top_n_selector
from components.formatters import format_month, get_fy_label
from components.analysis import (
    aggregate_by_frequency, filter_by_period, get_period_months, add_fy_columns,
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

st.title("Sub-Segment Analysis")

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


def _add_date_col(df):
    """Add a date column from year/month for filtering."""
    df = df.copy()
    df["date"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01"
    )
    return df


def _build_yoy_table(pivot_vol, ordered_labels, incomplete_periods, freq):
    """Build YoY growth table from a volume pivot. Returns DataFrame."""
    yoy_data = pivot_vol.copy().astype(float)
    row_labels = list(pivot_vol.index)
    yoy_result = pd.DataFrame(index=row_labels, columns=ordered_labels, dtype=object)

    for item in row_labels:
        for col_idx, col in enumerate(ordered_labels):
            curr = yoy_data.loc[item, col] if col in yoy_data.columns else None

            if freq == "monthly" and col_idx >= 12:
                prev_label = ordered_labels[col_idx - 12]
            elif freq == "quarterly" and col_idx >= 4:
                prev_label = ordered_labels[col_idx - 4]
            elif freq == "annual" and col_idx >= 1:
                prev_label = ordered_labels[col_idx - 1]
            else:
                prev_label = None

            if prev_label and prev_label in yoy_data.columns:
                prev = yoy_data.loc[item, prev_label]
                if pd.notna(curr) and pd.notna(prev) and prev > 0:
                    yoy_val = round(((curr / prev) - 1) * 100, 1)
                    yoy_result.loc[item, col] = _fmt_growth(yoy_val)
                else:
                    yoy_result.loc[item, col] = "\u2014"
            else:
                yoy_result.loc[item, col] = "\u2014"

    return yoy_result


def _build_share_table(pivot_vol, ordered_labels):
    """Build market share % table from a volume pivot (uses TOTAL row for denominator)."""
    if "TOTAL" not in pivot_vol.index:
        return None
    share_data = pivot_vol.copy().astype(float)
    totals = share_data.loc["TOTAL"]
    items = [i for i in share_data.index if i != "TOTAL"]
    share_result = pd.DataFrame(index=items, columns=ordered_labels, dtype=object)

    for item in items:
        for col in ordered_labels:
            curr = share_data.loc[item, col] if col in share_data.columns else None
            total = totals[col] if col in totals.index else None
            if pd.notna(curr) and pd.notna(total) and total > 0:
                share_result.loc[item, col] = f"{curr / total * 100:.1f}%"
            else:
                share_result.loc[item, col] = "\u2014"

    return share_result


# ====================
# SIDEBAR
# ====================
cat_kwargs = {"key": "pri_cat"}
if "pri_cat" not in st.session_state:
    cat_kwargs["index"] = CATEGORIES.index("2W") if "2W" in CATEGORIES else 0
selected_cat = st.sidebar.selectbox("Category", CATEGORIES, **cat_kwargs)

# Populate sub-segment list ordered by config
available_segments = get_primary_segments_list(selected_cat)
if not available_segments:
    st.warning(f"No primary sales sub-segment data for {selected_cat}. Import data first.")
    st.stop()

config_order = get_segment_order(selected_cat)
ordered_segments = [s for s in config_order if s in available_segments]
# Append any segments in data but not in config
remaining = [s for s in available_segments if s not in ordered_segments]
ordered_segments.extend(remaining)

seg_kwargs = {"key": "pss_segment"}
if "pss_segment" not in st.session_state and "Economy Segment" in ordered_segments:
    seg_kwargs["index"] = ordered_segments.index("Economy Segment")
selected_segment = st.sidebar.selectbox("Sub-Segment", ordered_segments, **seg_kwargs)

preset, ref_year, ref_month = primary_period_selector(key="pri_period")
if ref_year is None:
    st.warning("No data loaded. Upload data or run scraper in Data Management.")
    st.stop()

FREQ_MAP = {"Monthly": "monthly", "Quarterly": "quarterly", "Financial Year": "annual"}
freq_label = st.sidebar.selectbox("Frequency", list(FREQ_MAP.keys()), key="pri_freq")
freq = FREQ_MAP[freq_label]

top_n = top_n_selector(key="pss_topn")

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
if not has_primary_data(selected_cat):
    st.info(f"No primary sales data available for {selected_cat}.")
    st.stop()

# Sub-segment total (all segments, for share calculation)
all_seg_monthly = _add_date_col(get_primary_segment_monthly(selected_cat))
# Parent category total
cat_monthly = _add_date_col(get_primary_category_monthly(selected_cat))
# Model-level data for the selected segment
model_monthly = _add_date_col(get_primary_model_monthly(selected_cat, selected_segment))
# OEM-level data for the selected segment
oem_monthly = _add_date_col(get_primary_oem_in_segment(selected_cat, selected_segment))

if model_monthly.empty:
    st.info(f"No data for sub-segment '{selected_segment}' in {selected_cat}.")
    st.stop()

# Filter by period
start_date, end_date = get_period_months(preset, ref_year, ref_month)
filtered_model = filter_by_period(model_monthly, start_date, end_date)
filtered_oem = filter_by_period(oem_monthly, start_date, end_date)
filtered_seg = filter_by_period(all_seg_monthly, start_date, end_date)
filtered_cat = filter_by_period(cat_monthly, start_date, end_date)

if filtered_model.empty:
    st.info("No data for the selected period.")
    st.stop()

incomplete_periods = _get_incomplete_periods(filtered_model, freq)


# ======================================================================
# SECTION 1: Sub-Segment KPIs
# ======================================================================
st.subheader(f"{selected_segment} ({selected_cat}) \u2014 {format_month(ref_year, ref_month)}")

# Current month volume for segment
seg_this_month = filtered_seg[
    (filtered_seg["segment"] == selected_segment)
    & (filtered_seg["year"] == ref_year) & (filtered_seg["month"] == ref_month)
]
seg_vol = seg_this_month["volume"].sum()

# Prior year same month
seg_prev = all_seg_monthly[
    (all_seg_monthly["segment"] == selected_segment)
    & (all_seg_monthly["year"] == ref_year - 1) & (all_seg_monthly["month"] == ref_month)
]
seg_prev_vol = seg_prev["volume"].sum()
seg_yoy = round(((seg_vol / seg_prev_vol) - 1) * 100, 1) if seg_prev_vol > 0 else None

# Share of parent category
cat_this_month = filtered_cat[
    (filtered_cat["year"] == ref_year) & (filtered_cat["month"] == ref_month)
]
cat_vol = cat_this_month["volume"].sum()
seg_share = round(seg_vol / cat_vol * 100, 1) if cat_vol > 0 else None

k1, k2, k3 = st.columns(3)
k1.metric("Volume", _fmt_vol(seg_vol))
k2.metric("YoY Growth", _fmt_growth(seg_yoy) if seg_yoy is not None else "\u2014")
k3.metric(f"Share of {selected_cat}", f"{seg_share:.1f}%" if seg_share is not None else "\u2014")

st.divider()


# ======================================================================
# SECTION 2: Sub-Segment Volume + YoY Table (combined, no expander)
# ======================================================================
st.subheader("Sub-Segment Volume Trend")

seg_only = filtered_seg[filtered_seg["segment"] == selected_segment].copy()
seg_agg = aggregate_by_frequency(seg_only, freq)

# Apply YTD labels
if freq == "annual":
    seg_agg = _apply_ytd_labels(seg_agg, seg_only, freq)

seg_agg["label"] = seg_agg.apply(lambda r: _period_lbl(r, freq), axis=1)

ordered_labels_seg = seg_agg.sort_values("date")["label"].unique().tolist()

# Volume row
vol_row = {}
for _, r in seg_agg.iterrows():
    vol_row[r["label"]] = r["volume"]
vol_df = pd.DataFrame([vol_row], index=[selected_segment])
vol_df = vol_df.reindex(columns=ordered_labels_seg)

# Combine volume + YoY into one table with alternating rows
yoy_seg = _build_yoy_table(vol_df, ordered_labels_seg, incomplete_periods, freq)

combined_rows = []
for idx in vol_df.index:
    # Volume row
    vol_vals = {c: _fmt_vol(vol_df.loc[idx, c]) for c in ordered_labels_seg}
    vol_vals_row = pd.Series(vol_vals, name=f"{idx} - Volume")
    combined_rows.append(vol_vals_row)
    # YoY row
    yoy_vals = {c: yoy_seg.loc[idx, c] if idx in yoy_seg.index else "\u2014" for c in ordered_labels_seg}
    yoy_vals_row = pd.Series(yoy_vals, name=f"{idx} - YoY %")
    combined_rows.append(yoy_vals_row)

combined_df = pd.DataFrame(combined_rows)
combined_df.index.name = "Sub-Segment"
st.dataframe(combined_df.iloc[:, ::-1], width="stretch")

st.divider()


# ======================================================================
# SECTION 3: Model Volume Table
# ======================================================================
st.subheader("Model Volume")

model_agg_frames = []
for (oem, model), grp in filtered_model.groupby(["oem_name", "model_name"]):
    agg = aggregate_by_frequency(grp, freq)
    agg["oem_name"] = oem
    agg["model_name"] = model
    model_agg_frames.append(agg)

if not model_agg_frames:
    st.info("No model data for aggregation.")
    st.stop()

model_agg = pd.concat(model_agg_frames, ignore_index=True)

# Apply YTD labels
if freq == "annual":
    model_agg = _apply_ytd_labels(model_agg, filtered_model, freq)

model_agg["label"] = model_agg.apply(lambda r: _period_lbl(r, freq), axis=1)
ordered_labels_model = model_agg.sort_values("date")["label"].unique().tolist()

# Pivot: model as rows, periods as columns
model_agg["display_name"] = model_agg["model_name"] + " (" + model_agg["oem_name"] + ")"
pivot_model = model_agg.pivot_table(
    index="display_name", columns="label", values="volume", aggfunc="sum"
)
pivot_model = pivot_model.reindex(columns=ordered_labels_model)

# Sort by total volume desc
pivot_model["_total"] = pivot_model.sum(axis=1)
pivot_model = pivot_model.sort_values("_total", ascending=False).drop(columns=["_total"])

# Add TOTAL row
total_row = pivot_model.sum(axis=0)
total_row.name = "TOTAL"
pivot_model = pd.concat([pivot_model, total_row.to_frame().T])

# Combined Volume + YoY table
yoy_model = _build_yoy_table(pivot_model, ordered_labels_model, incomplete_periods, freq)

combined_model_rows = []
for idx in pivot_model.index:
    vol_vals = {c: _fmt_vol(pivot_model.loc[idx, c]) for c in ordered_labels_model}
    combined_model_rows.append(pd.Series(vol_vals, name=f"{idx} - Volume"))
    yoy_vals = {c: yoy_model.loc[idx, c] if idx in yoy_model.index else "\u2014" for c in ordered_labels_model}
    combined_model_rows.append(pd.Series(yoy_vals, name=f"{idx} - YoY %"))

combined_model_df = pd.DataFrame(combined_model_rows)
combined_model_df.index.name = "Model"
st.dataframe(combined_model_df.iloc[:, ::-1], width="stretch")



# ======================================================================
# SECTION 4: Model YoY Growth Table (expander)
# ======================================================================
st.divider()


# ======================================================================
# SECTION 6: OEM Volume Table
# ======================================================================
st.subheader("OEM Volume (within sub-segment)")

oem_agg_frames = []
for oem, grp in filtered_oem.groupby("oem_name"):
    agg = aggregate_by_frequency(grp, freq)
    agg["oem_name"] = oem
    oem_agg_frames.append(agg)

if not oem_agg_frames:
    st.info("No OEM data for aggregation.")
    st.stop()

oem_agg = pd.concat(oem_agg_frames, ignore_index=True)

# Apply YTD labels
if freq == "annual":
    oem_agg = _apply_ytd_labels(oem_agg, filtered_oem, freq)

oem_agg["label"] = oem_agg.apply(lambda r: _period_lbl(r, freq), axis=1)
ordered_labels_oem = oem_agg.sort_values("date")["label"].unique().tolist()

pivot_oem = oem_agg.pivot_table(
    index="oem_name", columns="label", values="volume", aggfunc="sum"
)
pivot_oem = pivot_oem.reindex(columns=ordered_labels_oem)

# Sort by total volume desc
pivot_oem["_total"] = pivot_oem.sum(axis=1)
pivot_oem = pivot_oem.sort_values("_total", ascending=False).drop(columns=["_total"])

# Add TOTAL row
total_row_oem = pivot_oem.sum(axis=0)
total_row_oem.name = "TOTAL"
pivot_oem = pd.concat([pivot_oem, total_row_oem.to_frame().T])

# Combined Volume + YoY table
yoy_oem = _build_yoy_table(pivot_oem, ordered_labels_oem, incomplete_periods, freq)

combined_oem_rows = []
for idx in pivot_oem.index:
    vol_vals = {c: _fmt_vol(pivot_oem.loc[idx, c]) for c in ordered_labels_oem}
    combined_oem_rows.append(pd.Series(vol_vals, name=f"{idx} - Volume"))
    yoy_vals = {c: yoy_oem.loc[idx, c] if idx in yoy_oem.index else "\u2014" for c in ordered_labels_oem}
    combined_oem_rows.append(pd.Series(yoy_vals, name=f"{idx} - YoY %"))

combined_oem_df = pd.DataFrame(combined_oem_rows)
combined_oem_df.index.name = "OEM"
st.dataframe(combined_oem_df.iloc[:, ::-1], width="stretch")





# ======================================================================
# SECTION 8: OEM Market Share Table (within sub-segment, expander)
# ======================================================================
with st.expander("OEM Market Share % (within sub-segment)"):
    share_oem = _build_share_table(pivot_oem, ordered_labels_oem)
    if share_oem is not None and not share_oem.empty:
        share_oem.index.name = "OEM"
        st.dataframe(share_oem.iloc[:, ::-1], width="stretch")
    else:
        st.info("Not enough data to compute shares.")

# -- OEM Market Share Line Chart --
st.markdown("**OEM Market Share Trend (within sub-segment)**")

from components.charts import market_share_line_chart

# pivot_oem has OEMs as rows (including TOTAL)
oem_leaf_p11 = pivot_oem.drop("TOTAL", errors="ignore") if "TOTAL" in pivot_oem.index else pivot_oem

# Top 7 + Others within this sub-segment
oem_totals_p11 = oem_leaf_p11.sum(axis=1).sort_values(ascending=False)
top7 = oem_totals_p11.head(7).index.tolist()

# Totals per period
period_totals = pivot_oem.loc["TOTAL"] if "TOTAL" in pivot_oem.index else pivot_oem.sum(axis=0)

share_long_p11 = []
for oem_n in top7:
    for col in ordered_labels_oem:
        if col in oem_leaf_p11.columns:
            v = oem_leaf_p11.loc[oem_n, col]
            t = period_totals.get(col, 0)
            if t > 0 and pd.notna(v):
                share_long_p11.append({
                    "oem_name": oem_n,
                    "label": col,
                    "share_pct": round(v / t * 100, 1),
                    "date_sort": ordered_labels_oem.index(col),
                })

# Others
other_oems_p11 = [o for o in oem_leaf_p11.index if o not in top7]
if other_oems_p11:
    others_vol = oem_leaf_p11.loc[other_oems_p11].sum(axis=0)
    for col in ordered_labels_oem:
        if col in others_vol.index:
            v = others_vol[col]
            t = period_totals.get(col, 0)
            if t > 0:
                share_long_p11.append({
                    "oem_name": "Others",
                    "label": col,
                    "share_pct": round(v / t * 100, 1),
                    "date_sort": ordered_labels_oem.index(col),
                })

if share_long_p11:
    sl_df = pd.DataFrame(share_long_p11).sort_values("date_sort")
    fig_p11 = market_share_line_chart(
        sl_df, title=f"{selected_segment} — OEM Market Share Trend",
        date_col="date_sort",
    )
    st.plotly_chart(fig_p11, width="stretch")
