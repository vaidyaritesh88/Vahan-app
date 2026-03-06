"""Number formatting and color utilities."""

MONTH_NAMES = [
    "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]


def format_units(value):
    """Format large numbers: 456.9K, 1.2M."""
    if value is None or value == 0:
        return "0"
    abs_val = abs(value)
    sign = "-" if value < 0 else ""
    if abs_val >= 1_000_000:
        return f"{sign}{abs_val / 1_000_000:.1f}M"
    elif abs_val >= 1_000:
        return f"{sign}{abs_val / 1_000:.1f}K"
    return f"{sign}{int(abs_val)}"


def format_pct(value, decimals=1):
    """Format as percentage string."""
    if value is None:
        return "N/A"
    return f"{value:+.{decimals}f}%"


def format_month(year, month):
    """Format year+month as 'Feb 2026'."""
    return f"{MONTH_NAMES[month]} {year}"


def format_fy(year, month):
    """Get fiscal year label for a given month. FY runs Apr-Mar."""
    fy = year if month >= 4 else year - 1
    return f"FY{str(fy + 1)[-2:]}"


def format_quarter(year, month):
    """Get quarter label. 1Q=Apr-Jun, 2Q=Jul-Sep, 3Q=Oct-Dec, 4Q=Jan-Mar."""
    if month in (4, 5, 6):
        q = 1
    elif month in (7, 8, 9):
        q = 2
    elif month in (10, 11, 12):
        q = 3
    else:
        q = 4
    fy = year if month >= 4 else year - 1
    return f"{q}Q{str(fy + 1)[-2:]}"


def delta_color(val):
    """Return color string for positive/negative values."""
    if val is None:
        return "gray"
    return "green" if val > 0 else ("red" if val < 0 else "gray")


# Color palette for OEMs
OEM_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    "#aec7e8", "#ffbb78", "#98df8a", "#ff9896", "#c5b0d5",
]


def get_oem_color(idx):
    """Get a color for an OEM by index."""
    return OEM_COLORS[idx % len(OEM_COLORS)]


# ── Fiscal Year Helpers ──

def get_fy_start_year(year, month):
    """Get the fiscal year start year. FY26 starts Apr 2025 → returns 2025."""
    return year if month >= 4 else year - 1


def get_fy_label(fy_start):
    """FY label from start year. 2025 → 'FY26'."""
    return f"FY{str(fy_start + 1)[-2:]}"


def get_fy_months(fy_start):
    """Return all 12 (year, month) tuples for a fiscal year starting in April."""
    months = []
    for m in range(4, 13):  # Apr-Dec
        months.append((fy_start, m))
    for m in range(1, 4):  # Jan-Mar
        months.append((fy_start + 1, m))
    return months


def get_fytd_months(year, month):
    """Return (year, month) tuples from FY start to the given month (inclusive)."""
    fy_start = get_fy_start_year(year, month)
    all_months = get_fy_months(fy_start)
    result = []
    for y, m in all_months:
        result.append((y, m))
        if y == year and m == month:
            break
    return result


def get_quarter_months(year, month):
    """Return the 3 (year, month) tuples for the quarter containing (year, month)."""
    if month in (4, 5, 6):
        return [(year, 4), (year, 5), (year, 6)]
    elif month in (7, 8, 9):
        return [(year, 7), (year, 8), (year, 9)]
    elif month in (10, 11, 12):
        return [(year, 10), (year, 11), (year, 12)]
    else:  # 1, 2, 3
        return [(year, 1), (year, 2), (year, 3)]


def get_prev_month(year, month):
    """Return (year, month) for the previous month."""
    if month == 1:
        return (year - 1, 12)
    return (year, month - 1)


def get_prev_quarter_end(year, month):
    """Return (year, month) for the last month of the previous quarter."""
    q_map = {4: (year, 3), 5: (year, 3), 6: (year, 3),
             7: (year, 6), 8: (year, 6), 9: (year, 6),
             10: (year, 9), 11: (year, 9), 12: (year, 9),
             1: (year - 1, 12), 2: (year - 1, 12), 3: (year - 1, 12)}
    return q_map[month]
