"""Number formatting and color utilities."""

MONTH_NAMES = [
    "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]


def format_units(value):
    """Format large numbers Indian style: 12.3L, 1.5Cr, 45.2K."""
    if value is None or value == 0:
        return "0"
    abs_val = abs(value)
    sign = "-" if value < 0 else ""
    if abs_val >= 1_00_00_000:
        return f"{sign}{abs_val / 1_00_00_000:.1f}Cr"
    elif abs_val >= 1_00_000:
        return f"{sign}{abs_val / 1_00_000:.1f}L"
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
