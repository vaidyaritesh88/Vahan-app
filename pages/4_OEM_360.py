"""Page 4: OEM 360 View — Analyst-grade deep-dive into any OEM.

Flow:  Snapshot → Sales Trend + YoY → Sub-Category Mix → Market Share → State Analysis
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import sys, os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_oem_all_categories, get_oem_monthly_all, get_oem_with_market_totals,
    get_oem_categories_list, get_subsegments_for_base, get_category_monthly_all,
    get_oem_state_distribution, get_oem_state_share_trend, has_state_data,
    get_latest_month, get_state_oem_monthly, get_state_category_monthly,
    get_oem_state_market_shares,
)
from components.filters import oem_selector, period_selector
from components.formatters import (
    format_units, format_month, format_pct, OEM_COLORS,
    get_fy_start_year, get_fy_label,
)
from components.charts import (
    dual_axis_bar_line, line_chart, horizontal_bar, LAYOUT_DEFAULTS,
)
from components.analysis import (
    compute_growth_rates, compute_fytd, compute_growth_series,
    add_fy_columns, filter_by_period, get_period_months, _pct_change,
)

init_db()

st.set_page_config(page_title="OEM 360", page_icon="🏢", layout="wide")
st.title("OEM 360 View")


# ──────────────────────────────────────
# HELPER: safe growth
# ──────────────────────────────────────
def _safe_growth(current, previous):
    if current is None or previous is None:
        return None
    if pd.isna(current) or pd.isna(previous) or previous <= 0:
        return None
    return round(((current / previous) - 1) * 100, 1)


# ──────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────
oem = oem_selector(key="oem360")
preset, ref_year, ref_month = period_selector(key="oem360_period")

if not oem or ref_year is None:
    st.warning("Select an OEM and time period.")
    st.stop()

show_subcat = st.sidebar.checkbox("Show sub-category detail", value=True, key="oem360_subcat")
st.sidebar.divider()

latest_year, latest_month = get_latest_month()
start_date, end_date = get_period_months(preset, ref_year, ref_month)

# ──────────────────────────────────────
# LOAD DATA
# ──────────────────────────────────────
oem_data = get_oem_monthly_all(oem)
if oem_data.empty:
    st.info(f"No data found for **{oem}**.")
    st.stop()

base_cats = get_oem_categories_list(oem)
cat_data = get_oem_all_categories(oem, ref_year, ref_month)
if cat_data.empty:
    st.warning(f"No data for {oem} in {format_month(ref_year, ref_month)}. Try a different month.")
    st.stop()

# Aggregate monthly totals — BASE categories only (subsegments are already included in base)
base_only = oem_data[oem_data["is_subsegment"] == 0]
agg_monthly = base_only.groupby(["year", "month", "date"])["volume"].sum().reset_index()
total_vol = cat_data[cat_data["category_code"].isin(
    base_cats["category_code"] if not base_cats.empty else []
)]["volume"].sum()

st.subheader(f"{oem}")


# ════════════════════════════════════════
# SECTION 1: COMPANY VOLUME SNAPSHOT
# ════════════════════════════════════════
growth = compute_growth_rates(agg_monthly, ref_year, ref_month)
fytd = compute_fytd(agg_monthly, ref_year, ref_month)

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        f"Volume ({format_month(ref_year, ref_month)})",
        format_units(total_vol),
    )
with col2:
    st.metric("YoY Growth", format_pct(growth["yoy_pct"]))
with col3:
    fy_start = get_fy_start_year(ref_year, ref_month)
    st.metric(f"FYTD ({get_fy_label(fy_start)})",
              format_units(fytd["fytd_vol"]) if fytd["fytd_vol"] else "N/A")
with col4:
    st.metric("FYTD YoY", format_pct(fytd["fytd_yoy_pct"]))

st.divider()


# ════════════════════════════════════════
# SECTION 2: RETAIL SALES TREND + YoY
# ════════════════════════════════════════
st.subheader("Retail Sales Trend")

trend = agg_monthly.sort_values("date").copy()
trend = filter_by_period(trend, start_date, end_date)

if not trend.empty:
    trend = compute_growth_series(trend)

    # Dual-axis chart: Volume bars + YoY line
    chart_df = trend.dropna(subset=["yoy_pct"]).copy()
    if not chart_df.empty:
        fig = dual_axis_bar_line(
            chart_df, x="date", bar_y="volume", line_y="yoy_pct",
            title=f"{oem} — Monthly Volume & YoY Growth",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Compact summary table (last 12 months, newest first)
    tbl = trend.tail(12).iloc[::-1].copy()
    display_trend = pd.DataFrame({
        "Month": tbl.apply(lambda r: format_month(int(r["year"]), int(r["month"])), axis=1).values,
        "Volume": tbl["volume"].apply(format_units).values,
        "YoY %": tbl["yoy_pct"].apply(lambda x: format_pct(x) if pd.notna(x) else "—").values,
    })
    with st.expander("Monthly Sales Table", expanded=False):
        st.dataframe(display_trend, use_container_width=True, hide_index=True)

st.divider()


# ════════════════════════════════════════
# SECTION 3: SUB-CATEGORY SALES MIX
# ════════════════════════════════════════
if show_subcat and not base_cats.empty:
    st.subheader("Sub-Category Sales Mix")

    for _, bcat in base_cats.iterrows():
        base_code = bcat["category_code"]
        base_name = bcat["category_name"]

        # Check if OEM has meaningful volume in this base category
        base_monthly = oem_data[oem_data["category_code"] == base_code].copy()
        if base_monthly.empty:
            continue

        base_ref = base_monthly[
            (base_monthly["year"] == ref_year) & (base_monthly["month"] == ref_month)
        ]
        if base_ref.empty or base_ref["volume"].iloc[0] < 10:
            continue

        # Get subsegments for this base
        subs_df = get_subsegments_for_base(base_code)
        if subs_df.empty:
            continue  # No subsegments — nothing to decompose

        sub_codes = subs_df["code"].tolist()
        sub_names = dict(zip(subs_df["code"], subs_df["name"]))

        st.markdown(f"#### {base_name}")

        # ── Build mix DataFrame ──
        # Get OEM monthly data for base + each subsegment
        base_series = base_monthly[["year", "month", "date", "volume"]].copy()
        base_series = base_series.rename(columns={"volume": "base_vol"})
        base_series = filter_by_period(base_series, start_date, end_date)

        mix_df = base_series.copy()
        for sc in sub_codes:
            sc_data = oem_data[oem_data["category_code"] == sc][["year", "month", "volume"]].copy()
            sc_data = sc_data.rename(columns={"volume": sc})
            mix_df = mix_df.merge(sc_data, on=["year", "month"], how="left")
            mix_df[sc] = mix_df[sc].fillna(0)

        # Compute ICE as residual
        mix_df["ICE"] = mix_df["base_vol"] - mix_df[sub_codes].sum(axis=1)
        mix_df["ICE"] = mix_df["ICE"].clip(lower=0)

        # Build long-form for 100% stacked chart
        fuel_types = ["ICE"] + sub_codes
        fuel_labels = {"ICE": f"ICE {base_name}"}
        fuel_labels.update({sc: sub_names.get(sc, sc) for sc in sub_codes})

        long_rows = []
        for _, row in mix_df.iterrows():
            base_total = row["base_vol"]
            if base_total <= 0:
                continue
            for ft in fuel_types:
                vol = row[ft]
                pct = round(vol / base_total * 100, 1) if base_total > 0 else 0
                long_rows.append({
                    "date": row["date"],
                    "year": int(row["year"]),
                    "month": int(row["month"]),
                    "type": fuel_labels.get(ft, ft),
                    "volume": vol,
                    "pct": pct,
                })

        if not long_rows:
            continue

        long_df = pd.DataFrame(long_rows)

        # ── 100% Stacked Bar Chart ──
        # Pivot for plotly stacked bars
        pivot_pct = long_df.pivot_table(index="date", columns="type", values="pct", fill_value=0)
        pivot_pct = pivot_pct.sort_index()

        colors = ["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A", "#19D3F3"]
        fig = go.Figure()
        for i, col_name in enumerate(pivot_pct.columns):
            fig.add_trace(go.Bar(
                x=pivot_pct.index, y=pivot_pct[col_name],
                name=col_name, marker_color=colors[i % len(colors)],
            ))
        fig.update_layout(
            **LAYOUT_DEFAULTS,
            barmode="stack",
            title=f"{oem} — {base_name} Mix (%)",
            yaxis_title="Share %",
            height=400,
        )
        fig.update_xaxes(title="")
        st.plotly_chart(fig, use_container_width=True)

        # ── Volume Table ──
        pivot_vol = long_df.pivot_table(index="date", columns="type", values="volume", fill_value=0)
        pivot_vol = pivot_vol.sort_index(ascending=False)

        # Format for display
        display_mix = pivot_vol.copy()
        display_mix.index = [format_month(d.year, d.month) for d in display_mix.index]
        display_mix["Total"] = display_mix.sum(axis=1)

        # YoY growth row for each column
        if len(pivot_vol) >= 13:
            latest_row = pivot_vol.iloc[-1]
            yoy_row = pivot_vol.iloc[-13] if len(pivot_vol) >= 13 else None
            if yoy_row is not None:
                growth_vals = {}
                for c in pivot_vol.columns:
                    growth_vals[c] = format_pct(_safe_growth(latest_row[c], yoy_row[c]))
                growth_vals["Total"] = format_pct(
                    _safe_growth(latest_row.sum(), yoy_row.sum())
                )

        # Show last 6 months
        disp_6 = display_mix.head(6).copy()
        for c in disp_6.columns:
            disp_6[c] = disp_6[c].apply(lambda x: format_units(x) if isinstance(x, (int, float, np.integer, np.floating)) else x)
        disp_6.index.name = "Month"

        with st.expander(f"{base_name} — Volume by Sub-Category", expanded=False):
            st.dataframe(disp_6, use_container_width=True)

            # YoY growth summary
            if len(pivot_vol) >= 13:
                st.caption("YoY Growth (latest month vs same month last year)")
                latest_row = pivot_vol.iloc[-1]
                yoy_row = pivot_vol.iloc[-13]
                growth_items = []
                for c in pivot_vol.columns:
                    g = _safe_growth(latest_row[c], yoy_row[c])
                    growth_items.append(f"**{c}**: {format_pct(g)}")
                total_g = _safe_growth(latest_row.sum(), yoy_row.sum())
                growth_items.append(f"**Total**: {format_pct(total_g)}")
                st.markdown(" · ".join(growth_items))

    st.divider()


# ════════════════════════════════════════
# SECTION 4: MARKET SHARE IN RELEVANT CATEGORIES
# ════════════════════════════════════════
st.subheader("Market Share")

if base_cats.empty:
    st.info("No base category data for market share analysis.")
else:
    for _, bcat in base_cats.iterrows():
        base_code = bcat["category_code"]
        base_name = bcat["category_name"]

        # Check OEM has volume in this category
        base_ref_data = oem_data[
            (oem_data["category_code"] == base_code) &
            (oem_data["year"] == ref_year) &
            (oem_data["month"] == ref_month)
        ]
        if base_ref_data.empty:
            continue

        st.markdown(f"#### {base_name}")

        # Overall share in base category
        share_data = get_oem_with_market_totals(oem, base_code)
        if share_data.empty:
            st.info(f"No share data for {base_name}.")
            continue

        share_data = filter_by_period(share_data, start_date, end_date)
        if share_data.empty:
            continue

        # Build combined share chart (overall + subsegments if enabled)
        traces_df = share_data[["date", "share_pct"]].copy()
        traces_df["type"] = f"Overall {base_name}"

        if show_subcat:
            subs_df = get_subsegments_for_base(base_code)
            for _, sub in subs_df.iterrows():
                sub_share = get_oem_with_market_totals(oem, sub["code"])
                if not sub_share.empty:
                    sub_share = filter_by_period(sub_share, start_date, end_date)
                    if not sub_share.empty:
                        sub_trace = sub_share[["date", "share_pct"]].copy()
                        sub_trace["type"] = sub["name"]
                        traces_df = pd.concat([traces_df, sub_trace], ignore_index=True)

        if not traces_df.empty:
            fig = line_chart(traces_df, x="date", y="share_pct", color="type",
                             title=f"{oem} — {base_name} Market Share", height=400)
            fig.update_yaxes(title="Market Share %")
            st.plotly_chart(fig, use_container_width=True)

        # Share table (last 6 months)
        share_tbl = share_data.sort_values("date", ascending=False).head(6)
        display_share = pd.DataFrame({
            "Month": share_tbl.apply(
                lambda r: format_month(int(r["year"]), int(r["month"])), axis=1
            ).values,
            "OEM Volume": share_tbl["oem_volume"].apply(format_units).values,
            "Market Total": share_tbl["total_volume"].apply(format_units).values,
            "Share %": share_tbl["share_pct"].apply(lambda x: f"{x:.1f}%").values,
        })
        with st.expander(f"{base_name} Share — Last 6 Months", expanded=False):
            st.dataframe(display_share, use_container_width=True, hide_index=True)

st.divider()


# ════════════════════════════════════════
# SECTION 5: STATE ANALYSIS
# ════════════════════════════════════════
if has_state_data():
    st.subheader("State Analysis")

    # Pick the primary category for state analysis
    cat_options_state = dict(zip(cat_data["category_name"], cat_data["category_code"]))
    # Filter to base categories only
    base_cat_codes = set(base_cats["category_code"]) if not base_cats.empty else set()
    state_cat_options = {k: v for k, v in cat_options_state.items() if v in base_cat_codes}
    if not state_cat_options:
        state_cat_options = cat_options_state  # Fallback

    if state_cat_options:
        selected_cat_name = st.selectbox(
            "Category for state analysis",
            list(state_cat_options.keys()), key="state_cat_360"
        )
        selected_cat = state_cat_options[selected_cat_name]

        # ── 5A: State Sales Mix (Top 15 states by contribution) ──
        state_market = get_oem_state_market_shares(oem, selected_cat, ref_year, ref_month)

        if not state_market.empty:
            col_a, col_b = st.columns(2)

            with col_a:
                top_states = state_market.head(15).copy()
                fig = horizontal_bar(
                    top_states, x="oem_volume", y="state",
                    title=f"Top States — {oem} ({selected_cat_name})",
                    text=top_states["contribution_pct"].apply(lambda x: f"{x:.1f}%"),
                )
                st.plotly_chart(fig, use_container_width=True)

            with col_b:
                # State market share table
                state_share_tbl = state_market.head(15).copy()
                display_state = pd.DataFrame({
                    "State": state_share_tbl["state"].values,
                    "Volume": state_share_tbl["oem_volume"].apply(format_units).values,
                    "Mkt Share %": state_share_tbl["share_pct"].apply(
                        lambda x: f"{x:.1f}%"
                    ).values,
                    "Contribution %": state_share_tbl["contribution_pct"].apply(
                        lambda x: f"{x:.1f}%"
                    ).values,
                })
                st.dataframe(display_state, use_container_width=True, hide_index=True)

            # ── 5B: State YoY Growth Table ──
            st.markdown("**State YoY Growth**")

            top_state_names = state_market.head(15)["state"].tolist()
            state_oem_monthly = get_state_oem_monthly(oem, selected_cat)

            if not state_oem_monthly.empty:
                # Get last 3 reference months for comparison
                ref_ym = ref_year * 100 + ref_month
                available_yms = sorted(state_oem_monthly["year"].astype(int) * 100 +
                                       state_oem_monthly["month"].astype(int))
                available_yms = [ym for ym in available_yms if ym <= ref_ym]
                # Take last 3 unique months
                unique_yms = sorted(set(available_yms), reverse=True)[:3]

                if unique_yms:
                    growth_rows = []
                    for state in top_state_names:
                        s_data = state_oem_monthly[state_oem_monthly["state"] == state].copy()
                        if s_data.empty:
                            continue
                        s_data["ym"] = s_data["year"].astype(int) * 100 + s_data["month"].astype(int)
                        vol_map = dict(zip(s_data["ym"], s_data["volume"]))

                        row = {"State": state}
                        for ym in unique_yms:
                            y = ym // 100
                            m = ym % 100
                            curr = vol_map.get(ym)
                            prev = vol_map.get((y - 1) * 100 + m)
                            g = _safe_growth(curr, prev)
                            label = format_month(y, m)
                            row[label] = format_pct(g) if g is not None else "—"
                        growth_rows.append(row)

                    if growth_rows:
                        st.dataframe(
                            pd.DataFrame(growth_rows),
                            use_container_width=True, hide_index=True,
                        )

            # ── 5C: State Share Trend ──
            with st.expander("State Share Trend", expanded=False):
                top_5_states = state_market.head(5)["state"].tolist()
                selected_state = st.selectbox(
                    "Select state for share trend",
                    top_5_states, key="state_share_trend_360",
                )
                if selected_state:
                    trend_data = get_oem_state_share_trend(
                        oem, selected_cat, selected_state, months=60,
                    )
                    if not trend_data.empty:
                        trend_data = filter_by_period(trend_data, start_date, end_date)
                        if not trend_data.empty:
                            fig = line_chart(
                                trend_data, x="date", y="share_pct",
                                title=f"Share Trend in {selected_state}",
                            )
                            fig.update_yaxes(title="Market Share %")
                            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No state data for this OEM/category combination.")
else:
    st.info("State-level data not yet available. Use the Vahan scraper to populate state data.")
