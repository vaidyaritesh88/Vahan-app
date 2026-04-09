# Chart Visualizations Across Dashboard Pages

**Date:** 2026-04-09
**Status:** Approved

## Context

The dashboard currently has strong table-based analytics but is sparse on visualizations. Adding line and column charts for market share and mix trends will make divergence and convergence patterns much easier to spot at a glance. Charts complement — don't replace — the existing tables.

## Goals

Add 7 charts across 5 pages, plus one section replacement on the Retail Subsegment Mix page.

## Design Decisions

### Top OEM Line Charts
- **Top 7 OEMs + Others** as individual lines on market share charts
- Rationale: 7 is the readability sweet spot for line charts. "Others" aggregate preserves total context. The existing `top_n_selector` is overkill for market share views.

### Column Charts (Segment Mix)
- **100% stacked columns** — each column sums to 100%, segments stack within
- Rationale: Standard visualization for mix analysis. Makes segment share evolution (e.g., UV vs PC share over time) immediately visible.
- **Leaf segments only** — no PC/UV/Motorcycle subtotal rows. Avoids double-counting.

### Color Palette
- **OEM lines:** Reuse existing `OEM_COLORS` array from `components/formatters.py` (15 colors cycling)
- **Subsegment lines:** Reuse `SUB_COLORS` dict (ICE=#636EFA, EV=#00CC96, CNG=#FFA15A, Hybrid=#AB63FA)
- **Category lines:** Reuse existing category colors from the 100% stacked chart in OEM 360

### Chart Conventions
- Height: 420px (matches `dual_axis_bar_line` default)
- Percentage format on Y-axis (0-100% for share charts)
- Unified hover showing all series at the same X position
- Legend at bottom for charts with 4+ series
- All charts respect the selected Monthly/Quarterly/Annual frequency
- YTDFY labels applied consistently (aligned with earlier fix)

## Chart Additions by Page

### 1. Retail Category Overview (`pages/1_Category_Overview.py`)

**New section after Category Mix % table:**
- **Category Mix % Trend** — line chart
- One line per category (PV, 2W, 3W, CV, TRACTORS)
- Y-axis: 0-100% share
- Shows how category mix evolved over the selected period

### 2. Retail Subsegment Mix (`pages/3_Subsegment_Mix.py`)

**New section after Subsegment Penetration table:**
- **Subsegment Share Trend** — line chart
- Lines: ICE, EV, CNG, Hybrid (as applicable for base category)
- Y-axis: 0-100% share of base category

**Replace existing "Top OEMs per Subsegment" section (currently horizontal bar):**
- **Timeseries table** — rows = top 7 OEMs + Others + TOTAL, columns = periods, values = absolute volume
- **Market Share % table** — same structure, values = share within subsegment
- **Market Share Trend** — line chart with top 7 OEMs + Others

### 3. Primary Sales Category Overview (`pages/9_Primary_Sales.py`)

**New chart under Segment Data section (after the three tables):**
- **Segment Mix % Trend** — 100% stacked column chart
- X-axis: periods
- Columns: stacked segments (Entry HB, Compact HB, ..., MUV for PV)
- Leaf segments only (no subtotals)

**New chart under OEM Data section (after the three tables):**
- **OEM Market Share Trend** — line chart
- Top 7 OEMs + Others
- Y-axis: 0-100% share

### 4. Primary Sub-Segment Analysis (`pages/11_Primary_SubSegment.py`)

**New chart after OEM tables:**
- **OEM Market Share Trend (within sub-segment)** — line chart
- Top 7 OEMs + Others within the selected sub-segment
- Y-axis: 0-100% share of sub-segment

### 5. Primary OEM 360 (`pages/12_Primary_OEM_360.py`)

**New chart after Overall Market Share table:**
- **Overall Market Share Trend** — single-line chart
- Shows selected OEM's market share % in the category over time

**New chart after Sub-Segment Market Share table:**
- **Sub-Segment Market Share Trend** — multi-line chart
- One line per sub-segment the OEM participates in
- Shows competitive position evolution across all sub-segments

## Implementation Approach

- Each chart is a self-contained section addition (no shared helpers needed beyond existing `aggregate_by_frequency`)
- Use `plotly.graph_objects.Figure()` with `go.Scatter()` for lines and `go.Bar()` for columns
- For 100% stacked columns: compute percentages first, then stack with `barmode="stack"`
- All charts use `width="stretch"` (the new Streamlit idiom)

## Files to Modify

| File | Changes |
|------|---------|
| `pages/1_Category_Overview.py` | +1 line chart |
| `pages/3_Subsegment_Mix.py` | +1 line chart, replace Top OEMs section (3 components) |
| `pages/9_Primary_Sales.py` | +1 column chart, +1 line chart |
| `pages/11_Primary_SubSegment.py` | +1 line chart |
| `pages/12_Primary_OEM_360.py` | +2 line charts |

## Verification Criteria

1. All charts render without errors
2. Line chart totals at each period ≤ 100% (share math correct)
3. 100% stacked column each bar sums to 100%
4. Top 7 + Others captures 100% of volume (Others is the residual)
5. Legend/hover readable; no overlapping labels
6. Frequency toggle (Monthly/Quarterly/Annual) works for all charts
7. YTDFY labels align with existing tables
