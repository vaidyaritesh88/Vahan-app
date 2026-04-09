"""Page 10: Primary vs Retail - Compare wholesale/dispatch vs Vahan registration volumes.

Detects inventory buildup at dealers by comparing primary (wholesale) sales
against retail (Vahan registration) data. Positive delta = inventory buildup,
negative delta = destocking.
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
    has_primary_data, get_last_scrape_info,
)
from components.filters import primary_period_selector
from components.formatters import format_units, format_month, format_pct, OEM_COLORS
from components.charts import dual_axis_bar_line
from components.analysis import (
    aggregate_by_frequency, filter_by_period, get_period_months, add_fy_columns,
)

init_db()

st.set_page_config(page_title="Primary vs Retail", page_icon="\u2696\ufe0f", layout="wide")

# CSS for right-aligned tables
st.markdown("""
<style>
div[data-testid="stDataFrame"] td { text-align: right !important; }
div[data-testid="stDataFrame"] th { text-align: center !important; }
div[data-testid="stDataFrame"] table { font-size: 0.85rem; }
</style>
""", unsafe_allow_html=True)

st.title("\u2696\ufe0f Primary vs Retail")


# ── Helpers ──
def _fmt_vol(val):
    if pd.isna(val) or val is None:
        return "\u2014"
    return f"{int(val):,}"


def _fmt_growth(val):
    if val is None or pd.isna(val):
        return "\u2014"
    return f"{val:+.1f}%"


def _fmt_delta_pct(val):
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


def _ensure_date(df):
    """Add a date column if missing."""
    if "date" not in df.columns:
        df["date"] = pd.to_datetime(
            df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2) + "-01"
        )
    return df


def _aggregate_comparison(primary_df, retail_df, freq):
    """Aggregate primary and retail data by frequency, then merge.

    Returns a merged DataFrame with columns:
      period_label, date, primary_vol, retail_vol, delta, delta_pct
    """
    # Aggregate primary
    if not primary_df.empty:
        p_agg = aggregate_by_frequency(primary_df.copy(), freq, vol_col="volume")
        p_agg["label"] = p_agg.apply(lambda r: _period_lbl(r, freq), axis=1)
        p_agg = p_agg[["label", "date", "volume"]].rename(columns={"volume": "primary_vol"})
    else:
        p_agg = pd.DataFrame(columns=["label", "date", "primary_vol"])

    # Aggregate retail
    if not retail_df.empty:
        r_agg = aggregate_by_frequency(retail_df.copy(), freq, vol_col="volume")
        r_agg["label"] = r_agg.apply(lambda r: _period_lbl(r, freq), axis=1)
        r_agg = r_agg[["label", "date", "volume"]].rename(columns={"volume": "retail_vol"})
    else:
        r_agg = pd.DataFrame(columns=["label", "date", "retail_vol"])

    if p_agg.empty and r_agg.empty:
        return pd.DataFrame()

    merged = pd.merge(p_agg, r_agg, on=["label", "date"], how="outer").sort_values("date")
    merged["delta"] = merged["primary_vol"] - merged["retail_vol"]
    merged["delta_pct"] = np.where(
        merged["retail_vol"] > 0,
        ((merged["primary_vol"] - merged["retail_vol"]) / merged["retail_vol"] * 100).round(1),
        np.nan,
    )
    return merged


# ══════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════
CATEGORIES = ["PV", "2W"]

selected_cat = st.sidebar.selectbox("Category", CATEGORIES, key="pri_cat")

preset, ref_year, ref_month = primary_period_selector(key="pri_period")
if ref_year is None:
    st.warning("No data loaded. Upload data or run scraper in Data Management.")
    st.stop()

FREQ_MAP = {"Monthly": "monthly", "Quarterly": "quarterly", "Financial Year": "annual"}
freq_label = st.sidebar.selectbox("Frequency", list(FREQ_MAP.keys()), key="pri_freq")
freq = FREQ_MAP[freq_label]

# OEM selector
if has_primary_data(selected_cat):
    primary_oem_raw = get_primary_oem_monthly(selected_cat)
    oem_list = sorted(primary_oem_raw["oem_name"].unique().tolist())
else:
    oem_list = []

oem_options = ["All OEMs"] + oem_list
selected_oem = st.sidebar.selectbox("OEM", oem_options, key="pvr_oem")

st.sidebar.divider()

_scrape_info = get_last_scrape_info()
if _scrape_info:
    with st.sidebar.expander("\U0001f504 Last Scraped", expanded=False):
        for si in _scrape_info:
            stype = si["scrape_type"].replace("national_", "N:").replace("state_", "S:")
            ts = si["last_completed"][:16] if si["last_completed"] else "Never"
            st.caption(f"**{stype}** \u2014 {ts}")


# ══════════════════════════════════════════════
# LOAD DATA
# ══════════════════════════════════════════════
if not has_primary_data(selected_cat):
    st.warning(
        f"No primary (wholesale) data available for **{selected_cat}**.\n\n"
        "Upload primary sales data via **Data Management** to use this page."
    )
    st.stop()

# Category-level data
primary_cat = get_primary_category_monthly(selected_cat)
primary_cat = _ensure_date(primary_cat)

retail_all = get_all_categories_monthly_from_vehcat()
retail_cat = retail_all[retail_all["category_code"] == selected_cat].copy()
retail_cat = _ensure_date(retail_cat)

if retail_cat.empty:
    st.warning(f"No retail (Vahan) data for **{selected_cat}**. Run the Vahan scraper first.")
    st.stop()

# Apply period filter
start_date, end_date = get_period_months(preset, ref_year, ref_month)
primary_cat_f = filter_by_period(primary_cat, start_date, end_date)
retail_cat_f = filter_by_period(retail_cat, start_date, end_date)

if primary_cat_f.empty and retail_cat_f.empty:
    st.info("No data for the selected period.")
    st.stop()

# OEM-level data (load once, filter later)
primary_oem_all = get_primary_oem_monthly(selected_cat)
primary_oem_all = _ensure_date(primary_oem_all)

retail_oem_all = get_category_oem_monthly_from_vehcat(selected_cat)
retail_oem_all = _ensure_date(retail_oem_all)

primary_oem_f = filter_by_period(primary_oem_all, start_date, end_date)
retail_oem_f = filter_by_period(retail_oem_all, start_date, end_date)


# ══════════════════════════════════════════════
# SECTION 1: Category-Level Comparison
# ══════════════════════════════════════════════
st.subheader(f"Section 1: {selected_cat} \u2014 Primary vs Retail")

comp = _aggregate_comparison(primary_cat_f, retail_cat_f, freq)

if not comp.empty:
    # Grouped bar chart
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=comp["label"], y=comp["primary_vol"],
        name="Primary (Wholesale)",
        marker_color="#636EFA",
    ))
    fig.add_trace(go.Bar(
        x=comp["label"], y=comp["retail_vol"],
        name="Retail (Vahan)",
        marker_color="#00CC96",
    ))
    fig.update_layout(
        barmode="group",
        title=f"{selected_cat} \u2014 Primary vs Retail Volumes",
        xaxis_title="Period",
        yaxis_title="Volume",
        height=480,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Data table
    tbl = comp[["label", "primary_vol", "retail_vol", "delta", "delta_pct"]].copy()
    tbl.columns = ["Period", "Primary Vol", "Retail Vol", "Delta (units)", "Delta %"]
    tbl["Primary Vol"] = tbl["Primary Vol"].apply(_fmt_vol)
    tbl["Retail Vol"] = tbl["Retail Vol"].apply(_fmt_vol)
    tbl["Delta (units)"] = tbl["Delta (units)"].apply(
        lambda v: f"{int(v):+,}" if pd.notna(v) else "\u2014"
    )
    tbl["Delta %"] = tbl["Delta %"].apply(_fmt_delta_pct)
    tbl = tbl.set_index("Period")
    st.dataframe(tbl, use_container_width=True)

    st.caption(
        "**Delta** = Primary - Retail. "
        "Positive = inventory buildup at dealers. "
        "Negative = destocking (retail exceeds wholesale)."
    )
else:
    st.info("Not enough overlapping data to compare Primary vs Retail.")

st.divider()


# ══════════════════════════════════════════════
# SECTION 2: OEM-Level Comparison (specific OEM)
# ══════════════════════════════════════════════
if selected_oem != "All OEMs":
    st.subheader(f"Section 2: {selected_oem} \u2014 Primary vs Retail")

    p_oem = primary_oem_f[primary_oem_f["oem_name"] == selected_oem].copy()
    r_oem = retail_oem_f[retail_oem_f["oem_name"] == selected_oem].copy()

    if p_oem.empty and r_oem.empty:
        st.info(f"No data for {selected_oem} in the selected period.")
    else:
        oem_comp = _aggregate_comparison(p_oem, r_oem, freq)

        if not oem_comp.empty:
            # Grouped bar chart
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=oem_comp["label"], y=oem_comp["primary_vol"],
                name="Primary (Wholesale)",
                marker_color="#636EFA",
            ))
            fig2.add_trace(go.Bar(
                x=oem_comp["label"], y=oem_comp["retail_vol"],
                name="Retail (Vahan)",
                marker_color="#00CC96",
            ))
            fig2.update_layout(
                barmode="group",
                title=f"{selected_oem} \u2014 Primary vs Retail Volumes",
                xaxis_title="Period",
                yaxis_title="Volume",
                height=480,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig2, use_container_width=True)

            # Data table
            tbl2 = oem_comp[["label", "primary_vol", "retail_vol", "delta", "delta_pct"]].copy()
            tbl2.columns = ["Period", "Primary Vol", "Retail Vol", "Delta (units)", "Delta %"]
            tbl2["Primary Vol"] = tbl2["Primary Vol"].apply(_fmt_vol)
            tbl2["Retail Vol"] = tbl2["Retail Vol"].apply(_fmt_vol)
            tbl2["Delta (units)"] = tbl2["Delta (units)"].apply(
                lambda v: f"{int(v):+,}" if pd.notna(v) else "\u2014"
            )
            tbl2["Delta %"] = tbl2["Delta %"].apply(_fmt_delta_pct)
            tbl2 = tbl2.set_index("Period")
            st.dataframe(tbl2, use_container_width=True)
        else:
            st.info(f"Not enough overlapping data for {selected_oem}.")

    st.divider()


# ══════════════════════════════════════════════
# SECTION 3: OEM Inventory Delta Table
# ══════════════════════════════════════════════
st.subheader("Section 3: OEM Inventory Delta Table")
st.caption("Delta % = (Primary - Retail) / Retail * 100. Top 10 OEMs by total primary volume.")

# Get last 6 periods from category-level comparison for column headers
if comp.empty:
    st.info("No comparison data available for the delta table.")
    st.stop()

last_periods = comp["label"].tolist()
if len(last_periods) > 6:
    last_periods = last_periods[-6:]

# Build OEM-level monthly merge
oem_merged = pd.merge(
    primary_oem_f.rename(columns={"volume": "primary_vol"}),
    retail_oem_f.rename(columns={"volume": "retail_vol"}),
    on=["year", "month", "oem_name", "date"],
    how="outer",
)

if oem_merged.empty:
    st.info("No OEM-level overlapping data for the delta table.")
    st.stop()

# Aggregate by frequency per OEM
oem_frames = []
for oem in oem_merged["oem_name"].unique():
    oem_slice = oem_merged[oem_merged["oem_name"] == oem].copy()

    # Build separate primary and retail slices for aggregation
    p_slice = oem_slice[["year", "month", "date", "primary_vol"]].dropna(subset=["primary_vol"]).copy()
    p_slice = p_slice.rename(columns={"primary_vol": "volume"})
    r_slice = oem_slice[["year", "month", "date", "retail_vol"]].dropna(subset=["retail_vol"]).copy()
    r_slice = r_slice.rename(columns={"retail_vol": "volume"})

    if not p_slice.empty:
        p_agg = aggregate_by_frequency(p_slice, freq, vol_col="volume")
        p_agg["label"] = p_agg.apply(lambda r: _period_lbl(r, freq), axis=1)
        p_agg = p_agg[["label", "volume"]].rename(columns={"volume": "primary_vol"})
    else:
        p_agg = pd.DataFrame(columns=["label", "primary_vol"])

    if not r_slice.empty:
        r_agg = aggregate_by_frequency(r_slice, freq, vol_col="volume")
        r_agg["label"] = r_agg.apply(lambda r: _period_lbl(r, freq), axis=1)
        r_agg = r_agg[["label", "volume"]].rename(columns={"volume": "retail_vol"})
    else:
        r_agg = pd.DataFrame(columns=["label", "retail_vol"])

    m = pd.merge(p_agg, r_agg, on="label", how="outer")
    m["oem_name"] = oem
    oem_frames.append(m)

if not oem_frames:
    st.info("No OEM-level data to build delta table.")
    st.stop()

oem_agg = pd.concat(oem_frames, ignore_index=True)

# Compute delta %
oem_agg["delta_pct"] = np.where(
    oem_agg["retail_vol"] > 0,
    ((oem_agg["primary_vol"] - oem_agg["retail_vol"]) / oem_agg["retail_vol"] * 100).round(1),
    np.nan,
)

# Top 10 OEMs by total primary volume
oem_totals = oem_agg.groupby("oem_name")["primary_vol"].sum().sort_values(ascending=False)
top_oems = oem_totals.head(10).index.tolist()

# Pivot: OEMs as rows, periods as columns
pivot = oem_agg[oem_agg["oem_name"].isin(top_oems)].pivot_table(
    index="oem_name", columns="label", values="delta_pct", aggfunc="first"
)

# Reindex to show only the last 6 periods and top OEM order
available_periods = [p for p in last_periods if p in pivot.columns]
if available_periods:
    pivot = pivot.reindex(columns=available_periods)
pivot = pivot.reindex([o for o in top_oems if o in pivot.index])

# Format cells
delta_display = pivot.copy()
for col in delta_display.columns:
    delta_display[col] = delta_display[col].apply(_fmt_delta_pct)

delta_display.index.name = "OEM"
st.dataframe(delta_display, use_container_width=True)
