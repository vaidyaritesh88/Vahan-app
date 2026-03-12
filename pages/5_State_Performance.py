"""Page 5: State Performance - State-level category analysis with volume trends and data tables."""
import streamlit as st
import pandas as pd
import numpy as np
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    has_state_data, get_states_with_data,
    get_state_all_categories_monthly, get_state_available_months,
)
from components.analysis import (
    aggregate_by_frequency, compute_growth_series,
    filter_by_period, get_period_months, add_fy_columns, PERIOD_PRESETS,
)
from components.formatters import format_month, format_quarter, format_fy
from components.charts import dual_axis_bar_line

init_db()

st.set_page_config(page_title="State Performance", page_icon="\U0001f4cd", layout="wide")
st.title("State Performance")

if not has_state_data():
    st.warning(
        "**No state-level data available yet.**\n\n"
        "Go to the **Data Management** page to run the Vahan scraper."
    )
    st.stop()


# ── Sidebar Controls ──────────────────────────
states = get_states_with_data()
if not states:
    st.warning("No states with data found.")
    st.stop()

selected_state = st.sidebar.selectbox("State", states, key="sp_state")

BASE_CATS = ["PV", "2W", "3W", "CV", "TRACTORS"]
selected_cat = st.sidebar.selectbox("Category (for chart)", BASE_CATS, index=1, key="sp_cat")

duration = st.sidebar.selectbox("Duration", list(PERIOD_PRESETS.keys()), key="sp_dur")

FREQ_MAP = {"Monthly": "monthly", "Quarterly": "quarterly", "Financial Year": "annual"}
freq_label = st.sidebar.selectbox("Frequency", list(FREQ_MAP.keys()), key="sp_freq")
freq = FREQ_MAP[freq_label]


# ── Load Data ─────────────────────────────────
all_data = get_state_all_categories_monthly(selected_state)
if all_data.empty:
    st.info(f"No category data for {selected_state}.")
    st.stop()

# Determine latest month from available data
latest = all_data.sort_values("date", ascending=False).iloc[0]
latest_year, latest_month = int(latest["year"]), int(latest["month"])

# Apply duration filter
start_date, end_date = get_period_months(duration, latest_year, latest_month)
filtered = filter_by_period(all_data, start_date, end_date)
if filtered.empty:
    st.info("No data for the selected duration.")
    st.stop()


# ── Period Label Helper ───────────────────────
def _period_label(row, freq):
    """Generate human-readable period label based on frequency."""
    y, m = int(row["year"]), int(row["month"])
    if freq == "monthly":
        return format_month(y, m)
    elif freq == "quarterly":
        return format_quarter(y, m)
    else:
        return format_fy(y, m)


# ══════════════════════════════════════════════
# SECTION 1: Volume Trend + YoY Chart
# ══════════════════════════════════════════════
cat_data = filtered[filtered["category_code"] == selected_cat].copy()
if cat_data.empty:
    st.warning(f"No {selected_cat} data for {selected_state} in this period.")
else:
    # Aggregate by frequency
    cat_agg = aggregate_by_frequency(cat_data, freq)

    # Compute YoY growth
    if freq == "monthly":
        cat_agg = compute_growth_series(cat_agg, "volume")
    else:
        # For quarterly/annual, YoY = compare to same period one year ago
        cat_agg = cat_agg.sort_values("date").reset_index(drop=True)
        periods_back = 4 if freq == "quarterly" else 1
        if len(cat_agg) > periods_back:
            cat_agg["yoy_pct"] = cat_agg["volume"].pct_change(
                periods=periods_back
            ).mul(100).round(1)
        else:
            cat_agg["yoy_pct"] = np.nan

    cat_agg["label"] = cat_agg.apply(lambda r: _period_label(r, freq), axis=1)

    # Drop rows where yoy_pct is NaN for cleaner chart
    chart_df = cat_agg.dropna(subset=["yoy_pct"]).copy()

    st.subheader(f"{selected_cat} — {selected_state}")
    if not chart_df.empty:
        fig = dual_axis_bar_line(
            chart_df, x="label", bar_y="volume", line_y="yoy_pct",
            title=f"{selected_cat} Registrations & YoY Growth",
            bar_name="Volume", line_name="YoY %",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough historical data to compute YoY growth. Showing volume only.")
        import plotly.express as px
        fig = px.bar(cat_agg, x="label", y="volume", title=f"{selected_cat} Registrations")
        fig.update_layout(height=420)
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ══════════════════════════════════════════════
# SECTION 2: Transposed Data Tables
# ══════════════════════════════════════════════
st.subheader(f"Category Data — {selected_state}")

# Aggregate all categories by frequency
tables_data = filtered.copy()
agg_frames = []
for cat in BASE_CATS:
    cat_slice = tables_data[tables_data["category_code"] == cat].copy()
    if cat_slice.empty:
        continue
    agg = aggregate_by_frequency(cat_slice, freq)
    agg["category_code"] = cat
    agg_frames.append(agg)

if not agg_frames:
    st.info("No data for tables.")
    st.stop()

tables_agg = pd.concat(agg_frames, ignore_index=True)
tables_agg["label"] = tables_agg.apply(lambda r: _period_label(r, freq), axis=1)

# Build pivot: categories as rows, periods as columns
pivot_vol = tables_agg.pivot_table(
    index="category_code", columns="label", values="volume", aggfunc="sum"
)

# Sort columns chronologically
ordered_labels = tables_agg.sort_values("date")["label"].unique().tolist()
pivot_vol = pivot_vol.reindex(columns=ordered_labels)

# Sort rows in standard order
cat_order = [c for c in BASE_CATS if c in pivot_vol.index]
pivot_vol = pivot_vol.reindex(cat_order)


# ── Table A: Volume ──
st.markdown("**Volume (units)**")
vol_display = pivot_vol.copy()
for col in vol_display.columns:
    vol_display[col] = vol_display[col].apply(
        lambda v: f"{int(v):,}" if pd.notna(v) and v > 0 else "\u2014"
    )
vol_display.index.name = "Category"
st.dataframe(vol_display, use_container_width=True)


# ── Table B: YoY Growth % ──
st.markdown("**YoY Growth %**")

# Compute YoY for each cell: compare to same period label offset
yoy_data = pivot_vol.copy().astype(float)
yoy_result = pd.DataFrame(index=cat_order, columns=ordered_labels, dtype=object)

for cat in cat_order:
    for col_idx, col in enumerate(ordered_labels):
        curr = yoy_data.loc[cat, col] if cat in yoy_data.index and col in yoy_data.columns else None
        # Find prior-year period
        if freq == "monthly" and col_idx >= 12:
            prev_label = ordered_labels[col_idx - 12]
        elif freq == "quarterly" and col_idx >= 4:
            prev_label = ordered_labels[col_idx - 4]
        elif freq == "annual" and col_idx >= 1:
            prev_label = ordered_labels[col_idx - 1]
        else:
            prev_label = None

        if prev_label and prev_label in yoy_data.columns:
            prev = yoy_data.loc[cat, prev_label]
            if pd.notna(curr) and pd.notna(prev) and prev > 0:
                yoy_val = round(((curr / prev) - 1) * 100, 1)
                yoy_result.loc[cat, col] = f"{yoy_val:+.1f}%"
            else:
                yoy_result.loc[cat, col] = "\u2014"
        else:
            yoy_result.loc[cat, col] = "\u2014"

yoy_result.index.name = "Category"
st.dataframe(yoy_result, use_container_width=True)


# ── Table C: Category Mix % ──
st.markdown("**Category Mix %**")

totals = pivot_vol.sum(axis=0)
mix_display = pivot_vol.copy()

for col in mix_display.columns:
    total = totals[col]
    if pd.notna(total) and total > 0:
        mix_display[col] = mix_display[col].apply(
            lambda v: f"{v / total * 100:.1f}%" if pd.notna(v) else "\u2014"
        )
    else:
        mix_display[col] = "\u2014"

mix_display.index.name = "Category"
st.dataframe(mix_display, use_container_width=True)
