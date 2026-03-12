# State Performance Page Redesign - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the existing State Performance page with a clean analyst-grade view: volume trend chart with YoY overlay + three transposed data tables (volume, YoY %, category mix %).

**Architecture:** Sidebar controls (state, category, duration, frequency) drive a single data pipeline that queries state_monthly, aggregates by frequency, filters by duration, and renders a dual-axis chart + three pandas-styled tables. All computation reuses existing analysis.py helpers.

**Tech Stack:** Streamlit, Plotly (via charts.py), pandas, SQLite

---

## Task 1: Add query helpers to database/queries.py

Add two new functions after line 448 (end of STATE section):

- `get_state_all_categories_monthly(state)` — returns all base category monthly volumes for a state (WHERE oem_name='__TOTAL__' AND category_code IN base cats)
- `get_state_available_months()` — returns distinct (year, month) pairs from state_monthly

## Task 2: Add FYTD to period presets in components/analysis.py

- Add `"FYTD": "fytd"` to PERIOD_PRESETS, remove "Last 2Y"
- Update `get_period_months()` to handle FYTD by computing FY start (April)

## Task 3: Rewrite pages/5_State_Performance.py

Full rewrite with sidebar controls + dual-axis chart + three transposed tables.

## Task 4: Integration test via streamlit run
