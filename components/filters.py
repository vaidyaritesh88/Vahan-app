"""Reusable Streamlit sidebar filter widgets."""
import streamlit as st
from database.queries import (
    get_available_months, get_latest_month, get_main_categories,
    get_all_categories, get_all_oems_for_category, get_all_oems,
    get_states_with_data, has_state_data,
)
from components.formatters import format_month, MONTH_NAMES
from components.analysis import PERIOD_PRESETS


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


# Major OEMs to show at top of dropdown (ordered by industry importance)
PRIORITY_OEMS = [
    # PV
    "Maruti Suzuki", "Hyundai", "Tata Motors", "Mahindra", "Toyota",
    "Kia", "Honda Cars", "MG Motor", "Skoda", "Volkswagen",
    # 2W
    "Hero MotoCorp", "Honda 2W", "TVS Motor", "Bajaj Auto",
    "Royal Enfield", "Suzuki 2W", "Yamaha",
    # EV 2W
    "Ola Electric", "Ather Energy",
    # CV
    "Ashok Leyland", "VECV", "Force Motors",
    # Tractor
    "Mahindra Tractors", "Sonalika", "Escorts", "TAFE", "John Deere",
    # 3W
    "Piaggio",
]


def oem_selector(category_code=None, key="oem", label="Select OEM"):
    """OEM dropdown with major OEMs listed first, then alphabetical."""
    if category_code:
        oems = get_all_oems_for_category(category_code)
    else:
        oems = get_all_oems()

    if not oems:
        return None

    oem_set = set(oems)
    # Priority OEMs that exist in data, in defined order
    top = [o for o in PRIORITY_OEMS if o in oem_set]
    # Remaining OEMs alphabetically
    rest = sorted([o for o in oems if o not in set(top)])
    # Combine with separator
    if top and rest:
        ordered = top + ["─" * 30] + rest  # horizontal line separator
    else:
        ordered = top + rest

    selected = st.sidebar.selectbox(label, ordered, key=key)
    # If user somehow selects the separator, default to first OEM
    if selected and "─" in selected:
        selected = ordered[0]
    return selected


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


def period_selector(key="period_preset", label="Analysis Period"):
    """Period preset selector. Returns (preset_name, year, month) for the reference month.

    Defaults to last COMPLETED month (not current month which may have partial data).
    """
    months = get_available_months()
    if not months:
        st.sidebar.warning("No data loaded yet.")
        return None, None, None

    preset = st.sidebar.selectbox(label, list(PERIOD_PRESETS.keys()), key=f"{key}_preset")

    # Default to last completed month (skip current month if it's in the list)
    from datetime import date as _date
    _today = _date.today()
    default_idx = 0
    for i, (y, m) in enumerate(months):
        if y == _today.year and m == _today.month:
            continue  # Skip current (partial) month
        default_idx = i
        break

    # Reference month selector
    options = [f"{format_month(y, m)}" for y, m in months]
    selected = st.sidebar.selectbox("Reference Month", options, index=default_idx, key=f"{key}_ref")
    idx = options.index(selected)
    year, month = months[idx]

    return preset, year, month


def frequency_selector(key="freq", label="View Frequency"):
    """Selector for monthly/quarterly/annual aggregation."""
    return st.selectbox(label, ["Monthly", "Quarterly", "Annual"], key=key)


def main_category_selector(key="main_cat", label="Select Category"):
    """Select from main categories (PV, 2W, 3W, CV, TRACTORS) - inline, not sidebar."""
    cats = get_main_categories()
    if cats.empty:
        return None, None
    options = dict(zip(cats["name"], cats["code"]))
    selected = st.sidebar.selectbox(label, list(options.keys()), key=key)
    return options[selected], selected

def primary_period_selector(key="ps_period", label="Analysis Period"):
    """Period preset selector using primary_sales data range (not national_monthly).

    Returns (preset_name, year, month).
    Session state is preserved across tab switches when all pages use the same key.
    """
    from database.queries import get_primary_available_months
    months = get_primary_available_months()
    if not months:
        st.sidebar.warning("No primary sales data loaded yet.")
        return None, None, None

    # Only set default index on first render — don't override session state on tab switch
    preset_key = f"{key}_preset"
    ref_key = f"{key}_ref"

    # Don't pass index if key already in session state (preserves user's selection)
    preset_kwargs = {"key": preset_key}
    if preset_key not in st.session_state:
        preset_kwargs["index"] = 0
    preset = st.sidebar.selectbox(label, list(PERIOD_PRESETS.keys()), **preset_kwargs)

    # Default to last completed month (only on first render)
    options = [f"{format_month(y, m)}" for y, m in months]

    if ref_key not in st.session_state:
        from datetime import date as _date
        _today = _date.today()
        default_idx = 0
        for i, (y, m) in enumerate(months):
            if y == _today.year and m == _today.month:
                continue
            default_idx = i
            break
        selected = st.sidebar.selectbox("Reference Month", options, index=default_idx, key=ref_key)
    else:
        selected = st.sidebar.selectbox("Reference Month", options, key=ref_key)
    idx = options.index(selected)
    year, month = months[idx]

    return preset, year, month

