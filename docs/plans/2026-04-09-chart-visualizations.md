# Chart Visualizations Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task.

**Goal:** Add 7 new charts across 5 pages + replace Top OEMs section on Retail Subsegment Mix page to improve visual analytics.

**Architecture:** Each chart is a self-contained section appended to or replacing existing page content. All charts use Plotly (`go.Figure`, `go.Scatter` for lines, `go.Bar` for columns). Top 7 + Others pattern for OEM charts. 100% stacked columns for segment mix.

**Tech Stack:** Streamlit 1.54, Plotly 5.18+, pandas 2.0+

**Testing note:** Streamlit pages are not easily unit-testable. We verify by (1) `py_compile` for syntax, (2) data simulation script that replicates page logic, (3) visual inspection post-deployment.

---

## Shared Helper Function

Before starting individual tasks, add a helper to `components/charts.py`:

```python
def market_share_line_chart(share_df, x_col="label", y_col="share_pct",
                             color_col="oem_name", title="", height=420,
                             colors=None):
    """Line chart for market share over time.

    Args:
        share_df: DataFrame with x_col, y_col, color_col columns
        colors: Optional list of colors (falls back to OEM_COLORS)

    Returns plotly Figure.
    """
    from components.formatters import OEM_COLORS
    palette = colors or OEM_COLORS

    fig = go.Figure()
    groups = share_df[color_col].unique().tolist()
    for i, grp in enumerate(groups):
        d = share_df[share_df[color_col] == grp].sort_values("date" if "date" in share_df.columns else x_col)
        fig.add_trace(go.Scatter(
            x=d[x_col], y=d[y_col],
            name=grp, mode="lines+markers",
            line=dict(width=2, color=palette[i % len(palette)]),
            marker=dict(size=5),
            hovertemplate="%{y:.1f}%<extra>" + grp + "</extra>",
        ))
    fig.update_layout(
        **LAYOUT_DEFAULTS, height=height, title=title,
        yaxis=dict(title="Share %", ticksuffix="%"),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
    )
    fig.update_xaxes(title="")
    return fig
```

This eliminates duplicate line chart code across 5+ pages.

---

## Task 1: Retail Category Mix % Line Chart

**File:** `pages/1_Category_Overview.py`

**Location:** Append after the existing Category Mix % table section.

**Implementation:**
```python
# Category Mix % Trend line chart
import plotly.graph_objects as go
from components.charts import market_share_line_chart, LAYOUT_DEFAULTS

st.markdown("**Category Mix % Trend**")

# Build long-form share DataFrame
mix_long = []
for cat in cat_order:
    for col in ordered_labels:
        total = totals.get(col, 0)
        val = pivot_no_total.loc[cat, col] if cat in pivot_no_total.index and col in pivot_no_total.columns else None
        if total and total > 0 and pd.notna(val):
            mix_long.append({
                "category": cat,
                "label": col,
                "share_pct": round(val / total * 100, 1),
            })

if mix_long:
    mix_df = pd.DataFrame(mix_long)
    # Preserve column order from ordered_labels
    mix_df["label"] = pd.Categorical(mix_df["label"], categories=ordered_labels, ordered=True)
    mix_df = mix_df.sort_values("label")

    fig = go.Figure()
    category_colors = {
        "PV": "#1f77b4", "2W": "#ff7f0e", "3W": "#2ca02c",
        "CV": "#d62728", "TRACTORS": "#9467bd",
    }
    for cat in cat_order:
        d = mix_df[mix_df["category"] == cat]
        if d.empty:
            continue
        fig.add_trace(go.Scatter(
            x=d["label"], y=d["share_pct"],
            name=cat, mode="lines+markers",
            line=dict(width=2.5, color=category_colors.get(cat, "#636EFA")),
            marker=dict(size=6),
            hovertemplate="%{y:.1f}%<extra>" + cat + "</extra>",
        ))
    fig.update_layout(
        **LAYOUT_DEFAULTS, height=420,
        title="Category Mix % over time",
        yaxis=dict(title="Share %", ticksuffix="%"),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.25),
    )
    fig.update_xaxes(title="")
    st.plotly_chart(fig, width="stretch")
```

**Verify:** `py_compile pages/1_Category_Overview.py` + simulate with PV/2W data.

**Commit:** `feat: add Category Mix % trend line chart to Retail Category Overview`

---

## Task 2: Retail Subsegment Share % Line Chart

**File:** `pages/3_Subsegment_Mix.py`

**Location:** Append after existing "Penetration %" table section.

**Implementation:** Build long-form DataFrame from `pivot_vol` (dropping TOTAL row), compute `share_pct = value / TOTAL * 100`, plot line chart using `SUB_COLORS` dict for ICE/EV/CNG/Hybrid colors.

**Commit:** `feat: add Subsegment Share % trend line chart to Subsegment Mix`

---

## Task 3: Replace Top OEMs Section on Retail Subsegment Mix

**File:** `pages/3_Subsegment_Mix.py`

**Location:** Current Section 6 (Top OEMs per Subsegment) — replace the horizontal bar chart + donut with:
1. Aggregate top 7 OEMs + Others from `get_all_oem_subsegment_monthly(sub_code)` filtered to period
2. Pivot to timeseries table (rows = OEMs, columns = periods)
3. Show Volume table (full numbers with commas)
4. Show Market Share % table (share within subsegment)
5. Show Market Share line chart using `market_share_line_chart` helper

**Iterate:** For each subsegment in `sub_codes` (EV_PV, PV_CNG, PV_HYBRID for PV, etc.), render inside an `st.expander()` with the subsegment name.

**Commit:** `feat: replace Top OEMs section with timeseries tables + share trend chart`

---

## Task 4: Primary Sales Segment Mix 100% Stacked Column

**File:** `pages/9_Primary_Sales.py`

**Location:** Append after Section 5 (Segment Mix % table expander, i.e., after the third segment table).

**Implementation:**
```python
# Segment Mix % 100% Stacked Column Chart
st.markdown("**Segment Mix % Trend**")

# Use pivot (without subtotals) for chart
seg_leaf_rows = [r for r in seg_row_order if r not in bold_rows]
pivot_leaf = seg_pivot.loc[seg_leaf_rows].copy()

# Compute percentages per column
col_totals = pivot_leaf.sum(axis=0)
pct_pivot = pivot_leaf.div(col_totals, axis=1) * 100

fig = go.Figure()
import plotly.express as px
colors = px.colors.qualitative.Pastel + px.colors.qualitative.Set3
for i, seg in enumerate(pct_pivot.index):
    fig.add_trace(go.Bar(
        x=pct_pivot.columns,
        y=pct_pivot.loc[seg],
        name=seg.strip(),  # strip leading indent spaces
        marker_color=colors[i % len(colors)],
        hovertemplate="%{y:.1f}%<extra>" + seg.strip() + "</extra>",
    ))
fig.update_layout(
    **LAYOUT_DEFAULTS, height=450,
    title=f"{selected_cat} Segment Mix % over time",
    barmode="stack",
    yaxis=dict(title="Share %", ticksuffix="%", range=[0, 100]),
    hovermode="x unified",
    legend=dict(orientation="h", yanchor="bottom", y=-0.3),
)
fig.update_xaxes(title="")
st.plotly_chart(fig, width="stretch")
```

**Commit:** `feat: add segment mix 100% stacked column chart to Primary Sales`

---

## Task 5: Primary OEM Market Share Line Chart

**File:** `pages/9_Primary_Sales.py`

**Location:** Append after Section 8 (OEM Market Share % expander).

**Implementation:**
1. Sort top 7 OEMs by total volume over the period
2. Aggregate "Others" = sum of all other OEMs
3. Build long-form share DataFrame: `oem | label | date | share_pct`
4. Call `market_share_line_chart(share_df, title=f"{selected_cat} OEM Market Share")`

**Commit:** `feat: add OEM market share line chart to Primary Sales`

---

## Task 6: Sub-Segment OEM Market Share Line Chart

**File:** `pages/11_Primary_SubSegment.py`

**Location:** Append after Section 8 (OEM Market Share expander).

**Implementation:** Same pattern as Task 5 but use `pivot_oem` which is already top-7-ish within the selected sub-segment. Compute share as `oem_vol / period_total * 100` and plot.

**Commit:** `feat: add OEM market share trend chart to Sub-Segment page`

---

## Task 7: Primary OEM 360 — Overall + Sub-Segment Share Line Charts

**File:** `pages/12_Primary_OEM_360.py`

**Location A:** Append after Section 3 (Overall Market Share table).

**Implementation A:** Single-line chart showing selected OEM's market share over time. Y-axis = share %.

**Location B:** Append after Section 5 (Sub-Segment Market Share table).

**Implementation B:** Multi-line chart, one line per sub-segment the OEM participates in. Use category-appropriate colors.

**Commit:** `feat: add OEM 360 market share line charts (overall + sub-segment)`

---

## Task 8: Final Verification + Push

**Steps:**
1. `py_compile` all 5 modified page files
2. Run simulation script that replicates each chart's data flow with real data
3. Verify top 7 + Others = 100% total
4. Verify 100% stacked columns sum to 100%
5. Push to remote

**Commit:** (no new commit, just `git push`)

---

## Implementation Notes

- **No shared state** between tasks — each can be implemented independently, but Task 3 (Subsegment Mix replacement) depends on the helper from the "Shared Helper Function" section being added to `components/charts.py` first.
- **Reuse existing helpers:** `aggregate_by_frequency`, `add_fy_columns`, `format_month`, `_fmt_vol`.
- **Color consistency:** Categories use predefined colors in task 1. OEMs use `OEM_COLORS` throughout. Subsegments use `SUB_COLORS` (already in page 3).
