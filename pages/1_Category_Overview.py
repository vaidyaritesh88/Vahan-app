"""Page 1: Category Overview - National volumes & YoY growth for all categories."""
import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import (
    get_category_summary, get_all_categories_monthly_trend,
    get_available_months, get_latest_month,
)
from components.filters import month_selector
from components.formatters import format_units, format_month, format_pct
from components.charts import monthly_bar_chart, yoy_bar_chart, line_chart

init_db()

st.set_page_config(page_title="Category Overview", page_icon="📊", layout="wide")
st.title("Category Overview")

# Sidebar filters
year, month = month_selector()
if year is None:
    st.warning("No data loaded. Upload an Excel file in Data Management.")
    st.stop()

st.sidebar.divider()
st.sidebar.markdown(f"**Showing:** {format_month(year, month)}")

# ── KPI Cards ──
summary = get_category_summary(year, month)
if summary.empty:
    st.info(f"No data for {format_month(year, month)}.")
    st.stop()

# Filter to main categories only for KPIs
main_cats = summary[~summary["category_code"].isin(["LCV", "MHCV", "PV_CNG", "PV_HYBRID"])]

st.subheader(f"Monthly Registrations - {format_month(year, month)}")

# Display KPIs in rows of 4
for i in range(0, len(main_cats), 4):
    chunk = main_cats.iloc[i:i+4]
    cols = st.columns(len(chunk))
    for col, (_, row) in zip(cols, chunk.iterrows()):
        with col:
            delta = f"{row['yoy_pct']:+.1f}%" if pd.notna(row.get("yoy_pct")) else None
            st.metric(
                label=row["category_name"],
                value=format_units(row["volume"]),
                delta=delta,
            )

st.divider()

# ── YoY Growth Chart ──
col1, col2 = st.columns(2)

with col1:
    yoy_data = main_cats.dropna(subset=["yoy_pct"])
    if not yoy_data.empty:
        fig = yoy_bar_chart(yoy_data, title=f"YoY Growth - {format_month(year, month)}")
        st.plotly_chart(fig, use_container_width=True)

# ── Monthly Trend ──
with col2:
    trend = get_all_categories_monthly_trend(months=12)
    if not trend.empty:
        trend["date"] = pd.to_datetime(trend[["year", "month"]].assign(day=1))
        fig = monthly_bar_chart(
            trend, x="date", y="volume", color="category_name",
            title="Monthly Volumes by Category (Last 12 Months)",
            barmode="group", height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Detailed Table ──
st.subheader("Detailed Summary")

# Build comparison table
table_data = []
for _, row in summary.iterrows():
    table_data.append({
        "Category": row["category_name"],
        "Code": row["category_code"],
        f"{format_month(year, month)}": int(row["volume"]),
        f"Prev Year ({format_month(year-1, month)})": int(row["prev_volume"]) if pd.notna(row.get("prev_volume")) else "-",
        "YoY %": f"{row['yoy_pct']:+.1f}%" if pd.notna(row.get("yoy_pct")) else "N/A",
    })

st.dataframe(
    pd.DataFrame(table_data),
    use_container_width=True,
    hide_index=True,
    column_config={
        f"{format_month(year, month)}": st.column_config.NumberColumn(format="%d"),
    },
)
