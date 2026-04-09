"""Page 10: Primary vs Retail - Compare wholesale/dispatch vs Vahan registration YoY growth.

Retail data is missing Telangana and Odisha, so absolute volume comparison is invalid.
Instead we compare YoY growth rates and a rebased volume index to detect
inventory buildup / destocking at dealers.
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
    get_all_categories_monthly_from_vehcat, get_category_oem_monthly_from_vehcat,
    has_primary_data, get_primary_available_months,
)
from components.filters import primary_period_selector
from components.formatters import format_month, OEM_COLORS
from components.analysis import (
    aggregate_by_frequency, filter_by_period, get_period_months, add_fy_columns,
)

init_db()

# ── CSS ──
st.markdown("""
<style>
div[data-testid="stDataFrame"] td { text-align: right !important; }
div[data-testid="stDataFrame"] th { text-align: center !important; }
div[data-testid="stDataFrame"] table { font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)

st.title("Primary vs Retail")

# ── Helpers ──────────────────────────────────────────────────────────────────

KEY_PV_OEMS = [
    "Maruti Suzuki", "Hyundai", "Tata Motors", "Mahindra", "Toyota",
    "Kia", "Honda Cars", "MG Motor", "Skoda VW",
]
KEY_2W_OEMS = [
    "Hero MotoCorp", "Honda 2W", "TVS Motor", "Bajaj Auto",
    "Royal Enfield", "Yamaha", "Suzuki 2W", "Ola Electric", "Ather Energy",
]


def _fmt_growth(val):
    if val is None or pd.isna(val):
        return "\u2014"
    return f"{val:+.1f}%"


def _fmt_index(val):
    if val is None or pd.isna(val):
        return "\u2014"
    return f"{val:.1f}"


def _fmt_gap_pp(val):
    """Format gap in percentage-points with color hint."""
    if val is None or pd.isna(val):
        return "\u2014"
    return f"{val:+.1f} pp"


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


def _ensure_date(df):
    if "date" not in df.columns:
        df["date"] = pd.to_datetime(
            df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01"
        )
    return df


def _compute_yoy(agg, vol_col="volume", freq="monthly"):
    """Add yoy_pct column to an aggregated DataFrame.

    For monthly: shift 12.  For quarterly/annual: shift 4 / 1 within sorted order.
    """
    agg = agg.sort_values("date").reset_index(drop=True)
    if freq == "monthly":
        shift = 12
    elif freq == "quarterly":
        shift = 4
    else:
        shift = 1
    agg["prev"] = agg[vol_col].shift(shift)
    agg["yoy_pct"] = np.where(
        agg["prev"] > 0,
        ((agg[vol_col] - agg["prev"]) / agg["prev"] * 100).round(1),
        np.nan,
    )
    agg.drop(columns=["prev"], inplace=True)
    return agg


def _rebase_index(agg, base_label, vol_col="volume"):
    """Rebase volume to 100 at *base_label* period."""
    match = agg.loc[agg["label"] == base_label, vol_col]
    if match.empty or match.iloc[0] in (0, np.nan, None):
        agg["index"] = np.nan
        return agg
    base_val = match.iloc[0]
    agg["index"] = (agg[vol_col] / base_val * 100).round(1)
    return agg


# ══════════════════════════════════════════════════════════════════════════════

# YoY axis clipping to handle COVID outliers (Apr-Jun 2021 base effects)
YOY_CLIP = (-80, 120)

def _clip_yoy(series):
    """Clip YoY% values and return (clipped_series, outlier_mask)."""
    import numpy as np
    clipped = series.clip(lower=YOY_CLIP[0], upper=YOY_CLIP[1])
    outliers = (series < YOY_CLIP[0]) | (series > YOY_CLIP[1])
    return clipped, outliers

# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
CATEGORIES = ["PV", "2W"]
cat_kwargs = {"key": "pri_cat"}
if "pri_cat" not in st.session_state:
    cat_kwargs["index"] = CATEGORIES.index("2W") if "2W" in CATEGORIES else 0
selected_cat = st.sidebar.selectbox("Category", CATEGORIES, **cat_kwargs)

preset, ref_year, ref_month = primary_period_selector(key="pri_period")
if ref_year is None:
    st.warning("No data loaded. Upload data or run scraper in Data Management.")
    st.stop()

FREQ_MAP = {"Monthly": "monthly", "Quarterly": "quarterly", "Financial Year": "annual"}
freq_label = st.sidebar.selectbox("Frequency", list(FREQ_MAP.keys()), key="pri_freq")
freq = FREQ_MAP[freq_label]

# ── Base Month for Index ──
avail_months = get_primary_available_months()  # descending
if avail_months:
    base_options = [format_month(y, m) for y, m in avail_months]
    # Default: Apr two FYs ago  (e.g. Apr 2024 if today is FY26)
    from datetime import date as _date
    _today = _date.today()
    _default_fy_start = _today.year - 2 if _today.month >= 4 else _today.year - 3
    _default_base = format_month(_default_fy_start, 4)
    _def_idx = base_options.index(_default_base) if _default_base in base_options else 0
    base_month_lbl = st.sidebar.selectbox(
        "Base Month (Index=100)", base_options,
        index=_def_idx if "pvr_base" not in st.session_state else None,
        key="pvr_base",
    )
else:
    base_month_lbl = None

# ── OEM selector ──
oem_choices = KEY_PV_OEMS if selected_cat == "PV" else KEY_2W_OEMS
oem_options = ["All OEMs (Category Level)"] + oem_choices
selected_oem = st.sidebar.selectbox("OEM", oem_options, key="pvr_oem")

st.sidebar.divider()

# ══════════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ══════════════════════════════════════════════════════════════════════════════
if not has_primary_data(selected_cat):
    st.warning(
        f"No primary (wholesale) data available for **{selected_cat}**.\n\n"
        "Upload primary sales data via **Data Management** to use this page."
    )
    st.stop()

# Load full (unfiltered) data for YoY computation — we need prior-year rows
is_oem = selected_oem != "All OEMs (Category Level)"

if is_oem:
    pri_raw = get_primary_oem_monthly(selected_cat)
    pri_raw = _ensure_date(pri_raw)
    pri_raw = pri_raw[pri_raw["oem_name"] == selected_oem].copy()
    ret_raw = get_category_oem_monthly_from_vehcat(selected_cat)
    ret_raw = _ensure_date(ret_raw)
    ret_raw = ret_raw[ret_raw["oem_name"] == selected_oem].copy()
else:
    pri_raw = get_primary_category_monthly(selected_cat)
    pri_raw = _ensure_date(pri_raw)
    ret_raw = get_all_categories_monthly_from_vehcat()
    ret_raw = _ensure_date(ret_raw)
    ret_raw = ret_raw[ret_raw["category_code"] == selected_cat].copy()

if pri_raw.empty:
    st.warning(f"No primary data for the selection.")
    st.stop()
if ret_raw.empty:
    st.warning(f"No retail (Vahan) data for **{selected_cat}**. Run the Vahan scraper first.")
    st.stop()

# Aggregate
pri_agg = aggregate_by_frequency(pri_raw.copy(), freq, vol_col="volume")
ret_agg = aggregate_by_frequency(ret_raw.copy(), freq, vol_col="volume")

pri_agg["label"] = pri_agg.apply(lambda r: _period_lbl(r, freq), axis=1)
ret_agg["label"] = ret_agg.apply(lambda r: _period_lbl(r, freq), axis=1)

# Incomplete periods
inc_pri = _get_incomplete_periods(pri_raw, freq)
inc_ret = _get_incomplete_periods(ret_raw, freq)

# YoY
pri_agg = _compute_yoy(pri_agg, "volume", freq)
ret_agg = _compute_yoy(ret_agg, "volume", freq)

# Suppress YoY for incomplete periods
for inc_set, agg_df in [(inc_pri, pri_agg), (inc_ret, ret_agg)]:
    if inc_set:
        agg_df.loc[agg_df["label"].isin(inc_set), "yoy_pct"] = np.nan

# Now apply period filter to the aggregated data
start_date, end_date = get_period_months(preset, ref_year, ref_month)
pri_f = pri_agg[(pri_agg["date"] >= start_date) & (pri_agg["date"] <= end_date)].copy()
ret_f = ret_agg[(ret_agg["date"] >= start_date) & (ret_agg["date"] <= end_date)].copy()

# Merge on label
merged = pd.merge(
    pri_f[["label", "date", "volume", "yoy_pct"]].rename(
        columns={"volume": "pri_vol", "yoy_pct": "pri_yoy"}
    ),
    ret_f[["label", "date", "volume", "yoy_pct"]].rename(
        columns={"volume": "ret_vol", "yoy_pct": "ret_yoy"}
    ),
    on=["label", "date"], how="outer",
).sort_values("date").reset_index(drop=True)

merged["gap_pp"] = (merged["pri_yoy"] - merged["ret_yoy"]).round(1)

if merged.empty:
    st.info("No overlapping data for the selected period.")
    st.stop()

entity_label = selected_oem if is_oem else selected_cat

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1: YoY Growth Comparison
# ══════════════════════════════════════════════════════════════════════════════
st.subheader(f"1. YoY Growth Comparison \u2014 {entity_label}")
st.caption(
    "Retail data excludes Telangana & Odisha, so absolute volumes are not comparable. "
    "YoY growth rates are comparable because the same states are missing in both years."
)

# ── Transposed table: rows = metrics, columns = periods ──
periods = merged["label"].tolist()
tbl_data = {
    "Primary YoY %": [_fmt_growth(v) for v in merged["pri_yoy"]],
    "Retail YoY %": [_fmt_growth(v) for v in merged["ret_yoy"]],
    "Gap (pp)": [_fmt_gap_pp(v) for v in merged["gap_pp"]],
}
tbl_df = pd.DataFrame(tbl_data, index=periods).T
tbl_df.index.name = "Metric"
st.dataframe(tbl_df, width="stretch")
st.caption(
    "**Gap** = Primary YoY - Retail YoY. "
    "Positive (red) = primary growing faster (inventory building). "
    "Negative (green) = retail growing faster (destocking)."
)

# ── Chart ──
fig1 = go.Figure()
fig1.add_trace(go.Scatter(
    x=merged["label"], y=merged["pri_yoy"],
    name="Primary YoY %", mode="lines+markers",
    line=dict(color="#636EFA", width=2.5),
    marker=dict(size=6),
))
fig1.add_trace(go.Scatter(
    x=merged["label"], y=merged["ret_yoy"],
    name="Retail YoY %", mode="lines+markers",
    line=dict(color="#00CC96", width=2.5),
    marker=dict(size=6),
))
fig1.add_hline(y=0, line_dash="dash", line_color="grey", line_width=1)
fig1.update_layout(
    title=f"{entity_label} \u2014 YoY Growth: Primary vs Retail",
    yaxis_title="YoY Growth %", yaxis_range=[YOY_CLIP[0] - 10, YOY_CLIP[1] + 10],
    xaxis_title="Period",
    height=460,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)
st.plotly_chart(fig1, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2: Rebased Volume Index
# ══════════════════════════════════════════════════════════════════════════════
st.subheader(f"2. Rebased Volume Index \u2014 {entity_label}")

if base_month_lbl is None:
    st.info("No base month available for index calculation.")
    st.stop()

st.markdown(
    f"Both series rebased to **100** at **{base_month_lbl}**. "
    "Divergence indicates cumulative inventory buildup or destocking."
)

# Build full (unfiltered) merged for index — need from base month onward
pri_full = pri_agg[["label", "date", "volume"]].rename(columns={"volume": "pri_vol"})
ret_full = ret_agg[["label", "date", "volume"]].rename(columns={"volume": "ret_vol"})
idx_merged = pd.merge(pri_full, ret_full, on=["label", "date"], how="outer").sort_values("date")

idx_merged = _rebase_index(idx_merged, base_month_lbl, vol_col="pri_vol")
idx_merged = idx_merged.rename(columns={"index": "pri_idx"})
idx_merged = _rebase_index(idx_merged, base_month_lbl, vol_col="ret_vol")
idx_merged = idx_merged.rename(columns={"index": "ret_idx"})
idx_merged["idx_gap"] = (idx_merged["pri_idx"] - idx_merged["ret_idx"]).round(1)

# Filter: from base month onward, within period
base_row = idx_merged.loc[idx_merged["label"] == base_month_lbl]
if not base_row.empty:
    base_date = base_row["date"].iloc[0]
    idx_show = idx_merged[
        (idx_merged["date"] >= base_date)
        & (idx_merged["date"] >= start_date)
        & (idx_merged["date"] <= end_date)
    ].copy()
else:
    # base month outside selected range — show what we can
    idx_show = idx_merged[
        (idx_merged["date"] >= start_date) & (idx_merged["date"] <= end_date)
    ].copy()

if idx_show.empty:
    st.info("Base month is outside the selected period range. Adjust the base month or period.")
    st.stop()

# ── Chart ──
fig2 = go.Figure()
fig2.add_trace(go.Scatter(
    x=idx_show["label"], y=idx_show["pri_idx"],
    name="Primary Index", mode="lines+markers",
    line=dict(color="#636EFA", width=2.5),
    marker=dict(size=6),
))
fig2.add_trace(go.Scatter(
    x=idx_show["label"], y=idx_show["ret_idx"],
    name="Retail Index", mode="lines+markers",
    line=dict(color="#00CC96", width=2.5),
    marker=dict(size=6),
))
fig2.add_hline(y=100, line_dash="dash", line_color="grey", line_width=1)
fig2.update_layout(
    title=f"{entity_label} \u2014 Rebased Volume Index (Base = {base_month_lbl})",
    yaxis_title="Index (100 = Base)",
    xaxis_title="Period",
    height=460,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)
st.plotly_chart(fig2, use_container_width=True)

# ── Transposed table ──
idx_periods = idx_show["label"].tolist()
idx_tbl = {
    "Primary Index": [_fmt_index(v) for v in idx_show["pri_idx"]],
    "Retail Index": [_fmt_index(v) for v in idx_show["ret_idx"]],
    "Gap (pts)": [_fmt_index(v) for v in idx_show["idx_gap"]],
}
idx_tbl_df = pd.DataFrame(idx_tbl, index=idx_periods).T
idx_tbl_df.index.name = "Metric"
st.dataframe(idx_tbl_df, width="stretch")
