"""Reusable Streamlit sidebar filter widgets."""
import streamlit as st
from database.queries import (
    get_available_months, get_latest_month, get_main_categories,
    get_all_categories, get_all_oems_for_category, get_all_oems,
    get_states_with_data, has_state_data,
)
from components.formatters import format_month, MONTH_NAMES


def month_selector(key="period", label="Select Month"):
    """Month/year selector dropdown. Returns (year, month)."""
    months = get_available_months()
    if not months:
        st.sidebar.warning("No data loaded yet.")
        return None, None

    options = [f"{format_month(y, m)}" for y, m in months]
    selected = st.sidebar.selectbox(label, options, key=key)
    idx = options.index(selected)
    return months[idx]


def category_selector(key="category", include_subsegments=False, label="Select Category"):
    """Category dropdown. Returns category code."""
    if include_subsegments:
        cats = get_all_categories()
    else:
        cats = get_main_categories()

    if cats.empty:
        return None

    options = dict(zip(cats["name"], cats["code"]))
    selected = st.sidebar.selectbox(label, list(options.keys()), key=key)
    return options[selected]


def oem_selector(category_code=None, key="oem", label="Select OEM"):
    """OEM dropdown. Returns OEM name."""
    if category_code:
        oems = get_all_oems_for_category(category_code)
    else:
        oems = get_all_oems()

    if not oems:
        return None

    return st.sidebar.selectbox(label, oems, key=key)


def top_n_selector(key="top_n", default=10):
    """Slider for top N OEMs."""
    return st.sidebar.slider("Top N OEMs", 3, 20, default, key=key)


def state_selector(category_code=None, key="state", label="Select State"):
    """State dropdown. Returns state name or None if no state data."""
    if not has_state_data():
        st.sidebar.info("No state-level data yet. Scrape Vahan to populate.")
        return None

    states = get_states_with_data(category_code)
    if not states:
        return None

    return st.sidebar.selectbox(label, states, key=key)


def base_category_selector(key="base_cat"):
    """Select base categories that have subsegments (PV, 2W, 3W)."""
    cats = get_all_categories()
    base_cats = cats[cats["is_subsegment"] == 0]
    # Only show categories that actually have subsegments
    sub_bases = cats[cats["is_subsegment"] == 1]["base_category_code"].unique()
    base_with_subs = base_cats[base_cats["code"].isin(sub_bases)]

    if base_with_subs.empty:
        return None

    options = dict(zip(base_with_subs["name"], base_with_subs["code"]))
    selected = st.sidebar.selectbox("Base Category", list(options.keys()), key=key)
    return options[selected]
