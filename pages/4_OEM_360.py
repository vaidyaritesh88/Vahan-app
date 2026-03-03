"""Page 4: OEM 360 View - Deep-dive into any OEM across categories and states."""
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_oem_all_categories, get_oem_share_in_categories,
    get_oem_volume_trend, get_oem_state_distribution,
    get_oem_state_share_trend, has_state_data, get_states_with_data,
    get_oem_growth_rates, get_oem_state_market_shares,
    get_oem_fuel_type_context, get_relevant_comparison_category,
)
from components.filters import month_selector, oem_selector
from components.formatters import format_units, format_month
from components.charts import (
    horizontal_bar, donut_chart, line_chart, monthly_bar_chart,
)

init_db()

st.set_page_config(page_title="OEM 360", page_icon="🏢", layout="wide")
st.title("OEM 360 View")

# Sidebar
oem = oem_selector(key="oem360")
year, month = month_selector(key="oem360_period")

if not oem or year is None:
    st.warning("Select an OEM and time period.")
    st.stop()

st.sidebar.divider()

# ── Category Presence ──
cat_data = get_oem_all_categories(oem, year, month)
if cat_data.empty:
    st.info(f"No data for {oem} in {format_month(year, month)}.")
    st.stop()

total_vol = cat_data["volume"].sum()

# Determine fuel-type context
fuel_ctx = get_oem_fuel_type_context(oem)

st.subheader(f"{oem} - {format_month(year, month)}")

# KPIs
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Volume", format_units(total_vol))
with col2:
    st.metric("Categories Active", len(cat_data))
with col3:
    top_cat = cat_data.iloc[0]
    st.metric("Largest Category", f"{top_cat['category_name']}")

st.divider()

# ── Growth Rates (fuel-type aware) ──
st.subheader("Growth Rates")
st.caption("Market share is computed against the relevant comparison universe (EV-only OEMs are compared within EV category).")

growth_rows = []
for base_cat, cat_codes in fuel_ctx.items():
    comparison_cat = get_relevant_comparison_category(oem, base_cat)
    growth = get_oem_growth_rates(comparison_cat, year, month, top_n=50)
    if not growth.empty:
        oem_row = growth[growth["oem_name"] == oem]
        if not oem_row.empty:
            row = oem_row.iloc[0].to_dict()
            row["comparison_category"] = comparison_cat
            growth_rows.append(row)

if growth_rows:
    growth_df = pd.DataFrame(growth_rows)

    def _fmt_g(val):
        if pd.isna(val):
            return "N/A"
        return f"{val:+.1f}%"

    display_g = growth_df.copy()
    display_g["Volume"] = display_g["volume"].apply(format_units)
    display_g["Share"] = display_g["share_pct"].apply(lambda v: f"{v:.1f}%")
    display_g["MoM"] = display_g["mom_pct"].apply(_fmt_g)
    display_g["QoQ"] = display_g["qoq_pct"].apply(_fmt_g)
    display_g["YoY"] = display_g["yoy_pct"].apply(_fmt_g)

    st.dataframe(
        display_g[["comparison_category", "Volume", "Share", "MoM", "QoQ", "YoY"]].rename(
            columns={"comparison_category": "Category"}
        ),
        use_container_width=True, hide_index=True,
    )

st.divider()

# ── Category Breakdown ──
col1, col2 = st.columns(2)

with col1:
    fig = horizontal_bar(cat_data, x="volume", y="category_name",
                         title=f"{oem} - Volume by Category")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = donut_chart(cat_data, names="category_name", values="volume",
                      title="Category Mix")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Market Share Trend Across Categories ──
st.subheader("Market Share Trend by Category")
st.caption("Shows share within the appropriate comparison universe per category.")
share_data = get_oem_share_in_categories(oem, months=24)
if not share_data.empty:
    fig = line_chart(share_data, x="date", y="share_pct", color="category_name",
                     title=f"{oem} - Market Share Trend (%)", height=450)
    fig.update_yaxes(title="Market Share %")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Volume Trend ──
st.subheader("Volume Trend")
vol_trend = get_oem_volume_trend(oem, months=24)
if not vol_trend.empty:
    vol_trend["date"] = pd.to_datetime(vol_trend[["year", "month"]].assign(day=1))
    fig = monthly_bar_chart(vol_trend, x="date", y="volume",
                            title=f"{oem} - Monthly Total Volume")
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── State Distribution (if state data available) ──
if has_state_data():
    st.subheader("State-Level Analysis")

    # Let user pick which category to see state distribution for
    cat_options = dict(zip(cat_data["category_name"], cat_data["category_code"]))
    selected_cat_name = st.selectbox("Category for state analysis", list(cat_options.keys()))
    selected_cat = cat_options[selected_cat_name]

    # Use fuel-type aware comparison category
    base_cats_map = {"EV_PV": "PV", "EV_2W": "2W", "EV_3W": "3W",
                     "PV_CNG": "PV", "PV_HYBRID": "PV"}
    comparison_cat = selected_cat  # default: compare within selected category

    # Show state market shares (not just volume)
    state_shares = get_oem_state_market_shares(oem, selected_cat, year, month)

    if not state_shares.empty:
        st.markdown(f"**{oem}'s market share in each state** (within {selected_cat_name})")

        col1, col2 = st.columns(2)
        with col1:
            # Bar chart: state market share
            top_states = state_shares.head(15).copy()
            top_states["label"] = top_states.apply(
                lambda r: f"{r['share_pct']:.1f}% share ({format_units(r['oem_volume'])})", axis=1
            )
            fig = horizontal_bar(top_states, x="share_pct", y="state",
                                 title=f"{oem} Market Share by State (%)", text="label")
            fig.update_xaxes(title="Market Share %")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Contribution: what % of OEM's total comes from each state
            top_contrib = state_shares.head(15).copy()
            top_contrib["label"] = top_contrib.apply(
                lambda r: f"{r['contribution_pct']:.1f}% ({format_units(r['oem_volume'])})", axis=1
            )
            fig = horizontal_bar(top_contrib, x="contribution_pct", y="state",
                                 title=f"{oem} Sales Contribution by State (%)", text="label")
            fig.update_xaxes(title="Contribution %")
            st.plotly_chart(fig, use_container_width=True)

        # Detailed state data table
        st.markdown("**State-Level Detail:**")
        display_states = state_shares.copy()
        display_states["OEM Volume"] = display_states["oem_volume"].apply(format_units)
        display_states["State Total"] = display_states["state_total"].apply(format_units)
        display_states["Market Share"] = display_states["share_pct"].apply(lambda v: f"{v:.1f}%")
        display_states["Contribution"] = display_states["contribution_pct"].apply(lambda v: f"{v:.1f}%")
        st.dataframe(
            display_states[["state", "OEM Volume", "State Total", "Market Share", "Contribution"]].rename(
                columns={"state": "State"}
            ),
            use_container_width=True, hide_index=True,
        )

        st.divider()

        # State share trend - pick a state
        st.markdown("**Track Share in a State Over Time:**")
        states = state_shares["state"].tolist()
        selected_state = st.selectbox("Select state", states[:15])
        if selected_state:
            share_trend = get_oem_state_share_trend(oem, selected_cat, selected_state, months=12)
            if not share_trend.empty:
                fig = line_chart(share_trend, x="date", y="share_pct",
                                 title=f"{oem} Share Trend in {selected_state}")
                fig.update_yaxes(title="Market Share %")
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No state data for this OEM/category. Scrape Vahan to populate.")
else:
    st.info("State-level data not yet available. Use the Vahan scraper to populate state data.")
