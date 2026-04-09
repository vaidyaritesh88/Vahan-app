"""All database query functions used by the Streamlit pages."""
import pandas as pd
from database.schema import get_connection
from components.formatters import (
    get_fy_start_year, get_fy_label, get_fytd_months,
    get_quarter_months, get_prev_month, get_prev_quarter_end,
    format_quarter, format_fy,
)


# OEMs that are primarily tractor manufacturers but register under LMV (Light Motor
# Vehicle) on the Vahan portal. These must be excluded from PV category queries to
# avoid inflating PV numbers by ~16%.
TRACTOR_OEMS = {
    'Mahindra Tractors', 'Sonalika', 'Escorts', 'TAFE', 'TAFE/Eicher',
    'John Deere', 'Case New Holland', 'Kubota', 'Captain', 'VST Tillers',
    'Preet', 'GROMAX AGRI EQUIPMENT LTD', 'INDO FARM EQUIPMENT LIMITED',
    'LOCAL TRAILER MANUFACTURER', 'ACTION CONSTRUCTION EQUIPMENT LTD.',
    'INTERNATIONAL TRACTORS LIMITED',
}


def _tractor_placeholders():
    """Return (placeholders_str, params_list) for excluding tractor OEMs.

    Usage: sql = f"oem_name NOT IN ({ph})"  with params
    """
    params = list(TRACTOR_OEMS)
    placeholders = ",".join(["?"] * len(params))
    return placeholders, params


def _query_df(sql, params=None):
    """Execute a query and return a pandas DataFrame."""
    conn = get_connection()
    df = pd.read_sql_query(sql, conn, params=params or [])
    conn.close()
    return df


# ──────────────────────────────────────────────
# DATA AVAILABILITY
# ──────────────────────────────────────────────

def get_available_months():
    """Get all (year, month) combinations that have data, sorted descending."""
    df = _query_df("""
        SELECT DISTINCT year, month FROM national_monthly
        WHERE volume > 0
        ORDER BY year DESC, month DESC
    """)
    return list(zip(df["year"], df["month"]))


def get_latest_month():
    """Get the most recent (year, month) with data."""
    months = get_available_months()
    return months[0] if months else (2026, 2)


def get_all_categories():
    """Get all category codes and names."""
    return _query_df("SELECT code, name, parent_code, is_subsegment, base_category_code, display_order FROM categories ORDER BY display_order")


def get_main_categories():
    """Get non-subsegment, non-child categories (PV, 2W, 3W, CV, TRACTORS)."""
    return _query_df("""
        SELECT code, name FROM categories
        WHERE is_subsegment = 0 AND parent_code IS NULL
        ORDER BY display_order
    """)


def get_all_oems_for_category(category_code):
    """Get all OEM names that have data for a category."""
    df = _query_df("""
        SELECT DISTINCT oem_name FROM national_monthly
        WHERE category_code = ? AND oem_name != 'Others' AND volume > 0
        ORDER BY oem_name
    """, [category_code])
    return df["oem_name"].tolist()


def get_all_oems():
    """Get all unique OEM names across all categories."""
    df = _query_df("""
        SELECT DISTINCT oem_name FROM national_monthly
        WHERE oem_name != 'Others' AND volume > 0
        ORDER BY oem_name
    """)
    return df["oem_name"].tolist()


# ──────────────────────────────────────────────
# CATEGORY OVERVIEW
# ──────────────────────────────────────────────

def get_category_summary(year, month):
    """Get total volume for each main category + EV categories for a given month, with YoY."""
    df = _query_df("""
        SELECT
            n.category_code,
            c.name as category_name,
            SUM(n.volume) as volume
        FROM national_monthly n
        JOIN categories c ON n.category_code = c.code
        WHERE n.year = ? AND n.month = ?
        GROUP BY n.category_code
        ORDER BY c.display_order
    """, [year, month])

    # Get same month last year for YoY
    prev_year = year - 1
    df_prev = _query_df("""
        SELECT category_code, SUM(volume) as prev_volume
        FROM national_monthly
        WHERE year = ? AND month = ?
        GROUP BY category_code
    """, [prev_year, month])

    if not df_prev.empty:
        df = df.merge(df_prev, on="category_code", how="left")
        df["yoy_pct"] = ((df["volume"] / df["prev_volume"]) - 1) * 100
    else:
        df["prev_volume"] = 0
        df["yoy_pct"] = None

    return df


def get_category_monthly_trend(category_code, months=24):
    """Get monthly total volumes for a category over the last N months."""
    return _query_df("""
        SELECT year, month, SUM(volume) as volume
        FROM national_monthly
        WHERE category_code = ? AND volume > 0
        GROUP BY year, month
        ORDER BY year DESC, month DESC
        LIMIT ?
    """, [category_code, months])


def get_all_categories_monthly_trend(months=12):
    """Get monthly totals for all main categories over last N months."""
    return _query_df("""
        SELECT n.category_code, c.name as category_name, n.year, n.month, SUM(n.volume) as volume
        FROM national_monthly n
        JOIN categories c ON n.category_code = c.code
        WHERE c.is_subsegment = 0 AND c.parent_code IS NULL
        GROUP BY n.category_code, n.year, n.month
        ORDER BY n.year, n.month
    """)


# ──────────────────────────────────────────────
# CATEGORY DRILLDOWN (OEM BREAKDOWN)
# ──────────────────────────────────────────────

def get_oem_volumes_for_category(category_code, year, month, top_n=10):
    """Get OEM-level volumes and market share for a category in a given month."""
    df = _query_df("""
        SELECT oem_name, volume
        FROM national_monthly
        WHERE category_code = ? AND year = ? AND month = ? AND volume > 0
        ORDER BY volume DESC
    """, [category_code, year, month])

    if df.empty:
        return df

    total = df["volume"].sum()
    df["share_pct"] = (df["volume"] / total * 100).round(1)

    # Group smaller OEMs into Others
    if top_n and len(df) > top_n:
        top = df.head(top_n).copy()
        others_vol = df.iloc[top_n:]["volume"].sum()
        others_row = pd.DataFrame([{
            "oem_name": "Others",
            "volume": others_vol,
            "share_pct": round(others_vol / total * 100, 1),
        }])
        df = pd.concat([top, others_row], ignore_index=True)

    return df


def get_oem_share_trend(category_code, top_n=7, months=24):
    """Get market share trends for top OEMs over time."""
    # Get top OEMs by latest month volume
    latest = get_available_months()
    if not latest:
        return pd.DataFrame()
    ly, lm = latest[0]

    top_oems = _query_df("""
        SELECT oem_name, volume FROM national_monthly
        WHERE category_code = ? AND year = ? AND month = ? AND oem_name != 'Others' AND volume > 0
        ORDER BY volume DESC LIMIT ?
    """, [category_code, ly, lm, top_n])["oem_name"].tolist()

    if not top_oems:
        return pd.DataFrame()

    placeholders = ",".join(["?"] * len(top_oems))
    df = _query_df(f"""
        SELECT n.oem_name, n.year, n.month, n.volume,
               SUM(n2.volume) as total_volume
        FROM national_monthly n
        JOIN (
            SELECT year, month, SUM(volume) as volume
            FROM national_monthly
            WHERE category_code = ?
            GROUP BY year, month
        ) n2 ON n.year = n2.year AND n.month = n2.month
        WHERE n.category_code = ? AND n.oem_name IN ({placeholders}) AND n.volume > 0
        GROUP BY n.oem_name, n.year, n.month
        ORDER BY n.year, n.month
    """, [category_code, category_code] + top_oems)

    if not df.empty:
        df["share_pct"] = (df["volume"] / df["total_volume"] * 100).round(1)
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))

    return df


def get_oem_monthly_trend(category_code, oem_name, months=24):
    """Get monthly volume trend for a specific OEM."""
    return _query_df("""
        SELECT year, month, volume
        FROM national_monthly
        WHERE category_code = ? AND oem_name = ? AND volume > 0
        ORDER BY year DESC, month DESC
        LIMIT ?
    """, [category_code, oem_name, months])


# ──────────────────────────────────────────────
# SUBSEGMENT MIX (EV / CNG / HYBRID)
# ──────────────────────────────────────────────

def get_subsegment_mix(base_category_code, year, month):
    """Get subsegment volumes for a base category (PV -> EV_PV, PV_CNG, PV_HYBRID)."""
    # Get base total
    base = _query_df("""
        SELECT SUM(volume) as total FROM national_monthly
        WHERE category_code = ? AND year = ? AND month = ?
    """, [base_category_code, year, month])

    base_total = base["total"].iloc[0] if not base.empty else 0

    # Get subsegment totals
    subs = _query_df("""
        SELECT c.code, c.name, SUM(n.volume) as volume
        FROM national_monthly n
        JOIN categories c ON n.category_code = c.code
        WHERE c.base_category_code = ? AND n.year = ? AND n.month = ?
        GROUP BY c.code
    """, [base_category_code, year, month])

    if not subs.empty and base_total > 0:
        subs["penetration_pct"] = (subs["volume"] / base_total * 100).round(2)
    else:
        subs["penetration_pct"] = 0

    subs["base_total"] = base_total
    return subs


def get_subsegment_trend(base_category_code, months=24):
    """Get monthly subsegment penetration trends."""
    df = _query_df("""
        SELECT
            n.year, n.month,
            n.category_code,
            c.name as subsegment_name,
            SUM(n.volume) as volume
        FROM national_monthly n
        JOIN categories c ON n.category_code = c.code
        WHERE c.base_category_code = ?
        GROUP BY n.year, n.month, n.category_code
        ORDER BY n.year, n.month
    """, [base_category_code])

    # Get base totals
    base = _query_df("""
        SELECT year, month, SUM(volume) as base_total
        FROM national_monthly
        WHERE category_code = ?
        GROUP BY year, month
    """, [base_category_code])

    if not df.empty and not base.empty:
        df = df.merge(base, on=["year", "month"], how="left")
        df["penetration_pct"] = (df["volume"] / df["base_total"] * 100).round(2)
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))

    return df


def get_subsegment_oem_breakdown(subsegment_code, year, month, top_n=7):
    """Get OEM breakdown within a subsegment (e.g., top EV PV makers)."""
    return get_oem_volumes_for_category(subsegment_code, year, month, top_n)


# ──────────────────────────────────────────────
# OEM 360 VIEW
# ──────────────────────────────────────────────

def get_oem_all_categories(oem_name, year, month):
    """Get an OEM's volumes across all categories it participates in."""
    return _query_df("""
        SELECT n.category_code, c.name as category_name, n.volume
        FROM national_monthly n
        JOIN categories c ON n.category_code = c.code
        WHERE n.oem_name = ? AND n.year = ? AND n.month = ? AND n.volume > 0
        ORDER BY n.volume DESC
    """, [oem_name, year, month])


def get_oem_share_in_categories(oem_name, months=24):
    """Get an OEM's market share trend across all its categories."""
    # First find which categories this OEM participates in
    cats = _query_df("""
        SELECT DISTINCT category_code FROM national_monthly
        WHERE oem_name = ? AND volume > 0
    """, [oem_name])["category_code"].tolist()

    if not cats:
        return pd.DataFrame()

    placeholders = ",".join(["?"] * len(cats))
    df = _query_df(f"""
        SELECT n.category_code, c.name as category_name, n.year, n.month, n.volume,
               t.total_volume
        FROM national_monthly n
        JOIN categories c ON n.category_code = c.code
        JOIN (
            SELECT category_code, year, month, SUM(volume) as total_volume
            FROM national_monthly
            WHERE category_code IN ({placeholders})
            GROUP BY category_code, year, month
        ) t ON n.category_code = t.category_code AND n.year = t.year AND n.month = t.month
        WHERE n.oem_name = ? AND n.volume > 0
        ORDER BY n.year, n.month
    """, cats + [oem_name])

    if not df.empty:
        df["share_pct"] = (df["volume"] / df["total_volume"] * 100).round(1)
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))

    return df


def get_oem_volume_trend(oem_name, category_code=None, months=24):
    """Get an OEM's monthly volume trend, optionally filtered by category."""
    if category_code:
        return _query_df("""
            SELECT year, month, volume FROM national_monthly
            WHERE oem_name = ? AND category_code = ? AND volume > 0
            ORDER BY year DESC, month DESC LIMIT ?
        """, [oem_name, category_code, months])
    else:
        return _query_df("""
            SELECT year, month, SUM(volume) as volume FROM national_monthly
            WHERE oem_name = ? AND volume > 0
            GROUP BY year, month
            ORDER BY year DESC, month DESC LIMIT ?
        """, [oem_name, months])


# ──────────────────────────────────────────────
# STATE / REGIONAL (from state_monthly table)
# ──────────────────────────────────────────────

def get_state_volumes(category_code, year, month):
    """Get state-level volumes for a category."""
    return _query_df("""
        SELECT state, SUM(volume) as volume
        FROM state_monthly
        WHERE category_code = ? AND year = ? AND month = ?
              AND oem_name = '__TOTAL__'
        GROUP BY state
        ORDER BY volume DESC
    """, [category_code, year, month])


def get_state_oem_breakdown(category_code, state, year, month, top_n=10):
    """Get OEM breakdown within a specific state for a category."""
    df = _query_df("""
        SELECT oem_name, volume
        FROM state_monthly
        WHERE category_code = ? AND state = ? AND year = ? AND month = ?
              AND volume > 0 AND oem_name <> '__TOTAL__'
        ORDER BY volume DESC
    """, [category_code, state, year, month])

    if df.empty:
        return df

    total = df["volume"].sum()
    df["share_pct"] = (df["volume"] / total * 100).round(1)

    if top_n and len(df) > top_n:
        top = df.head(top_n).copy()
        others_vol = df.iloc[top_n:]["volume"].sum()
        others_row = pd.DataFrame([{"oem_name": "Others", "volume": others_vol,
                                     "share_pct": round(others_vol / total * 100, 1)}])
        df = pd.concat([top, others_row], ignore_index=True)

    return df


def get_oem_state_distribution(oem_name, category_code, year, month):
    """Get an OEM's sales distribution across states."""
    return _query_df("""
        SELECT state, volume
        FROM state_monthly
        WHERE oem_name = ? AND category_code = ? AND year = ? AND month = ?
        ORDER BY volume DESC
    """, [oem_name, category_code, year, month])


def get_oem_state_share_trend(oem_name, category_code, state, months=12):
    """Get an OEM's market share trend in a specific state."""
    df = _query_df("""
        SELECT s.year, s.month, s.volume as oem_volume,
               t.total_volume
        FROM state_monthly s
        JOIN (
            SELECT year, month, SUM(volume) as total_volume
            FROM state_monthly
            WHERE category_code = ? AND state = ?
            GROUP BY year, month
        ) t ON s.year = t.year AND s.month = t.month
        WHERE s.oem_name = ? AND s.category_code = ? AND s.state = ?
        ORDER BY s.year, s.month
    """, [category_code, state, oem_name, category_code, state])

    if not df.empty:
        df["share_pct"] = (df["oem_volume"] / df["total_volume"] * 100).round(1)
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))

    return df


def has_state_data():
    """Check if any state-level data exists."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM state_monthly")
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0


def get_states_with_data(category_code=None):
    """Get states that have data, optionally for a specific category."""
    if category_code:
        df = _query_df("""
            SELECT DISTINCT state FROM state_monthly
            WHERE category_code = ? AND volume > 0
            ORDER BY state
        """, [category_code])
    else:
        df = _query_df("SELECT DISTINCT state FROM state_monthly WHERE volume > 0 ORDER BY state")
    return df["state"].tolist()




def get_state_all_categories_monthly(state):
    """Get monthly volumes for all base categories in a specific state.

    Returns DataFrame: category_code, year, month, volume, date
    Filters to __TOTAL__ rows and base categories only.
    """
    df = _query_df("""
        SELECT category_code, year, month, volume
        FROM state_monthly
        WHERE state = ? AND oem_name = '__TOTAL__'
              AND category_code IN ('PV', '2W', '3W', 'CV', 'TRACTORS')
              AND volume > 0
        ORDER BY category_code, year, month
    """, [state])
    if not df.empty:
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    return df


def get_state_available_months():
    """Get available (year, month) pairs from state_monthly data, sorted desc."""
    df = _query_df("""
        SELECT DISTINCT year, month FROM state_monthly
        WHERE volume > 0 AND oem_name = '__TOTAL__'
        ORDER BY year DESC, month DESC
    """)
    return list(zip(df["year"], df["month"]))

# ──────────────────────────────────────────────
# OEM 360 ENHANCED / INDUSTRY ANALYSIS
# ──────────────────────────────────────────────

def get_oem_monthly_all(oem_name):
    """Get all monthly volume data for an OEM across all categories."""
    df = _query_df("""
        SELECT n.category_code, c.name as category_name,
               c.is_subsegment, c.base_category_code,
               n.year, n.month, n.volume
        FROM national_monthly n
        JOIN categories c ON n.category_code = c.code
        WHERE n.oem_name = ? AND n.volume > 0
        ORDER BY n.year, n.month
    """, [oem_name])
    if not df.empty:
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    return df


def get_category_monthly_all(category_code):
    """Get all monthly total volumes for a category (all OEMs summed)."""
    df = _query_df("""
        SELECT year, month, SUM(volume) as volume
        FROM national_monthly
        WHERE category_code = ? AND volume > 0
        GROUP BY year, month
        ORDER BY year, month
    """, [category_code])
    if not df.empty:
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    return df


def get_oem_with_market_totals(oem_name, category_code):
    """Get OEM monthly volumes alongside category totals for share computation."""
    df = _query_df("""
        SELECT n.year, n.month, n.volume as oem_volume, t.total_volume
        FROM national_monthly n
        JOIN (
            SELECT year, month, SUM(volume) as total_volume
            FROM national_monthly
            WHERE category_code = ?
            GROUP BY year, month
        ) t ON n.year = t.year AND n.month = t.month
        WHERE n.oem_name = ? AND n.category_code = ? AND n.volume > 0
        ORDER BY n.year, n.month
    """, [category_code, oem_name, category_code])
    if not df.empty:
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
        df["share_pct"] = (df["oem_volume"] / df["total_volume"] * 100).round(2)
    return df


def get_oem_categories_list(oem_name):
    """Get the list of base categories an OEM participates in (non-subsegment)."""
    return _query_df("""
        SELECT DISTINCT n.category_code, c.name as category_name
        FROM national_monthly n
        JOIN categories c ON n.category_code = c.code
        WHERE n.oem_name = ? AND n.volume > 0
              AND c.is_subsegment = 0 AND c.parent_code IS NULL
        ORDER BY c.display_order
    """, [oem_name])


def get_subsegments_for_base(base_category_code):
    """Get subsegment category codes for a base category."""
    return _query_df("""
        SELECT code, name FROM categories
        WHERE base_category_code = ? AND is_subsegment = 1
        ORDER BY display_order
    """, [base_category_code])


def get_category_oem_volumes_all(category_code):
    """Get all monthly OEM volumes for a category (for market share over time)."""
    return _query_df("""
        SELECT oem_name, year, month, volume
        FROM national_monthly
        WHERE category_code = ? AND volume > 0 AND oem_name != 'Others'
        ORDER BY year, month
    """, [category_code])


def get_state_oem_monthly(oem_name, category_code):
    """Get OEM's monthly state-level volumes."""
    df = _query_df("""
        SELECT state, year, month, volume
        FROM state_monthly
        WHERE oem_name = ? AND category_code = ? AND volume > 0
        ORDER BY year, month
    """, [oem_name, category_code])
    if not df.empty:
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    return df


def get_state_category_monthly(category_code):
    """Get category monthly totals by state (for state-level share)."""
    df = _query_df("""
        SELECT state, year, month, SUM(volume) as total_volume
        FROM state_monthly
        WHERE category_code = ? AND volume > 0
        GROUP BY state, year, month
        ORDER BY year, month
    """, [category_code])
    if not df.empty:
        df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    return df


def get_ev_penetration_all(base_category_code):
    """Get EV penetration data for a base category over all available months."""
    subs = get_subsegments_for_base(base_category_code)
    ev_codes = [c for c in subs["code"].tolist() if c.startswith("EV_")]
    if not ev_codes:
        return pd.DataFrame()

    placeholders = ",".join(["?"] * len(ev_codes))
    ev_df = _query_df(f"""
        SELECT year, month, SUM(volume) as ev_volume
        FROM national_monthly
        WHERE category_code IN ({placeholders}) AND volume > 0
        GROUP BY year, month
        ORDER BY year, month
    """, ev_codes)

    base_df = _query_df("""
        SELECT year, month, SUM(volume) as base_volume
        FROM national_monthly
        WHERE category_code = ? AND volume > 0
        GROUP BY year, month
        ORDER BY year, month
    """, [base_category_code])

    if ev_df.empty or base_df.empty:
        return pd.DataFrame()

    df = ev_df.merge(base_df, on=["year", "month"], how="inner")
    df["penetration_pct"] = (df["ev_volume"] / df["base_volume"] * 100).round(2)
    df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))
    return df


def get_top_oems_for_period(category_code, start_year, start_month, end_year, end_month, top_n=10):
    """Get top OEMs by total volume for a period range."""
    df = _query_df("""
        SELECT oem_name, SUM(volume) as volume
        FROM national_monthly
        WHERE category_code = ? AND volume > 0 AND oem_name != 'Others'
              AND (year * 100 + month) >= ? AND (year * 100 + month) <= ?
        GROUP BY oem_name
        ORDER BY volume DESC
        LIMIT ?
    """, [category_code, start_year * 100 + start_month,
          end_year * 100 + end_month, top_n])
    if not df.empty:
        total = df["volume"].sum()
        df["share_pct"] = (df["volume"] / total * 100).round(1)
    return df


# ──────────────────────────────────────────────
# DATA MANAGEMENT
# ──────────────────────────────────────────────

def get_data_freshness():
    """Get data freshness info: latest month per category."""
    return _query_df("""
        SELECT category_code, MAX(year * 100 + month) as latest_ym,
               COUNT(DISTINCT oem_name) as oem_count,
               SUM(volume) as total_records
        FROM national_monthly
        WHERE volume > 0
        GROUP BY category_code
        ORDER BY category_code
    """)


def get_load_history():
    """Get recent data load history."""
    return _query_df("SELECT * FROM load_log ORDER BY loaded_at DESC LIMIT 20")


def get_record_counts():
    """Get total records per table."""
    conn = get_connection()
    counts = {}
    for table in ["national_monthly", "state_monthly", "weekly_trends"]:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        counts[table] = cursor.fetchone()[0]
    conn.close()
    return counts


def get_scrape_log_summary():
    """Get scrape log grouped by category/state/year with status."""
    return _query_df("""
        SELECT category_code, state, year, status,
               MAX(completed_at) as last_run,
               SUM(rows_inserted) as total_rows
        FROM scrape_log
        GROUP BY category_code, state, year, status
        ORDER BY last_run DESC
    """)


def get_state_data_freshness():
    """Get state-level data freshness: latest month per category/state."""
    return _query_df("""
        SELECT category_code, state,
               MAX(year * 100 + month) as latest_ym,
               COUNT(DISTINCT oem_name) as oem_count,
               SUM(volume) as total_volume
        FROM state_monthly
        WHERE volume > 0
        GROUP BY category_code, state
        ORDER BY category_code, state
    """)


# ──────────────────────────────────────────────
# OEM GROWTH RATES (MoM, QoQ, YoY)
# ──────────────────────────────────────────────

def _sum_volume_for_months(category_code, month_list):
    """Get OEM volumes summed over a list of (year, month) tuples."""
    if not month_list:
        return pd.DataFrame(columns=["oem_name", "volume"])
    conditions = " OR ".join(["(year = ? AND month = ?)"] * len(month_list))
    params = [category_code]
    for y, m in month_list:
        params.extend([y, m])
    df = _query_df(f"""
        SELECT oem_name, SUM(volume) as volume
        FROM national_monthly
        WHERE category_code = ? AND ({conditions}) AND volume > 0
        GROUP BY oem_name
    """, params)
    return df


def get_oem_growth_rates(category_code, year, month, top_n=10):
    """Get MoM, QoQ, YoY growth rates for top N OEMs in a category.

    Returns DataFrame with columns: oem_name, volume, share_pct,
        mom_pct, qoq_pct, yoy_pct
    """
    # Current month volumes
    current = _query_df("""
        SELECT oem_name, volume FROM national_monthly
        WHERE category_code = ? AND year = ? AND month = ? AND volume > 0
        ORDER BY volume DESC
    """, [category_code, year, month])

    if current.empty:
        return pd.DataFrame()

    total = current["volume"].sum()
    current["share_pct"] = (current["volume"] / total * 100).round(1)

    # Top N
    if top_n and len(current) > top_n:
        current = current.head(top_n)

    oem_list = current["oem_name"].tolist()

    # Previous month (MoM)
    pm_y, pm_m = get_prev_month(year, month)
    prev_month = _query_df("""
        SELECT oem_name, volume as prev_m_vol FROM national_monthly
        WHERE category_code = ? AND year = ? AND month = ? AND volume > 0
    """, [category_code, pm_y, pm_m])

    # Current quarter and previous quarter (QoQ)
    curr_q_months = get_quarter_months(year, month)
    pq_end_y, pq_end_m = get_prev_quarter_end(year, month)
    prev_q_months = get_quarter_months(pq_end_y, pq_end_m)

    curr_q = _sum_volume_for_months(category_code, curr_q_months)
    curr_q = curr_q.rename(columns={"volume": "curr_q_vol"})
    prev_q = _sum_volume_for_months(category_code, prev_q_months)
    prev_q = prev_q.rename(columns={"volume": "prev_q_vol"})

    # Same month last year (YoY)
    prev_year = _query_df("""
        SELECT oem_name, volume as prev_y_vol FROM national_monthly
        WHERE category_code = ? AND year = ? AND month = ? AND volume > 0
    """, [category_code, year - 1, month])

    # Merge all
    df = current.merge(prev_month, on="oem_name", how="left")
    df = df.merge(curr_q, on="oem_name", how="left")
    df = df.merge(prev_q, on="oem_name", how="left")
    df = df.merge(prev_year, on="oem_name", how="left")

    # Calculate growth rates
    df["mom_pct"] = ((df["volume"] / df["prev_m_vol"]) - 1).mul(100).round(1)
    df["qoq_pct"] = ((df["curr_q_vol"] / df["prev_q_vol"]) - 1).mul(100).round(1)
    df["yoy_pct"] = ((df["volume"] / df["prev_y_vol"]) - 1).mul(100).round(1)

    return df[["oem_name", "volume", "share_pct", "mom_pct", "qoq_pct", "yoy_pct"]]


# ──────────────────────────────────────────────
# QUARTERLY / FYTD / ANNUAL AGGREGATIONS
# ──────────────────────────────────────────────

def get_oem_quarterly_share(category_code, top_n=7, num_quarters=8):
    """Get OEM market share aggregated by FY quarter.

    Returns DataFrame: oem_name, quarter_label, volume, total_volume, share_pct
    """
    df = _query_df("""
        SELECT oem_name, year, month, volume
        FROM national_monthly
        WHERE category_code = ? AND volume > 0
    """, [category_code])

    if df.empty:
        return pd.DataFrame()

    # Assign quarter labels
    df["quarter"] = df.apply(lambda r: format_quarter(r["year"], r["month"]), axis=1)
    df["fy_start"] = df.apply(lambda r: get_fy_start_year(r["year"], r["month"]), axis=1)
    df["q_num"] = df["month"].map({4:1,5:1,6:1,7:2,8:2,9:2,10:3,11:3,12:3,1:4,2:4,3:4})
    df["sort_key"] = df["fy_start"] * 10 + df["q_num"]

    # Aggregate by quarter
    q_data = df.groupby(["oem_name", "quarter", "sort_key"], as_index=False)["volume"].sum()
    q_totals = q_data.groupby(["quarter", "sort_key"], as_index=False)["volume"].sum()
    q_totals = q_totals.rename(columns={"volume": "total_volume"})

    q_data = q_data.merge(q_totals, on=["quarter", "sort_key"], how="left")
    q_data["share_pct"] = (q_data["volume"] / q_data["total_volume"] * 100).round(1)

    # Keep only the latest N quarters
    quarters_sorted = q_data[["quarter", "sort_key"]].drop_duplicates().sort_values("sort_key", ascending=False)
    keep_quarters = quarters_sorted.head(num_quarters)["quarter"].tolist()
    q_data = q_data[q_data["quarter"].isin(keep_quarters)]

    # Filter to top N OEMs (by latest quarter volume)
    latest_q = quarters_sorted.iloc[0]["quarter"]
    top_oems = q_data[q_data["quarter"] == latest_q].nlargest(top_n, "volume")["oem_name"].tolist()
    q_data = q_data[q_data["oem_name"].isin(top_oems)]
    q_data = q_data.sort_values("sort_key")

    return q_data


def get_oem_annual_share(category_code, top_n=7):
    """Get OEM market share aggregated by full fiscal year.

    Returns DataFrame: oem_name, fy_label, volume, total_volume, share_pct
    """
    df = _query_df("""
        SELECT oem_name, year, month, volume
        FROM national_monthly
        WHERE category_code = ? AND volume > 0
    """, [category_code])

    if df.empty:
        return pd.DataFrame()

    df["fy_start"] = df.apply(lambda r: get_fy_start_year(r["year"], r["month"]), axis=1)
    df["fy_label"] = df["fy_start"].apply(get_fy_label)

    # Aggregate by FY
    fy_data = df.groupby(["oem_name", "fy_label", "fy_start"], as_index=False)["volume"].sum()
    fy_totals = fy_data.groupby(["fy_label", "fy_start"], as_index=False)["volume"].sum()
    fy_totals = fy_totals.rename(columns={"volume": "total_volume"})

    fy_data = fy_data.merge(fy_totals, on=["fy_label", "fy_start"], how="left")
    fy_data["share_pct"] = (fy_data["volume"] / fy_data["total_volume"] * 100).round(1)

    # Top N by latest FY
    latest_fy = fy_data["fy_start"].max()
    top_oems = fy_data[fy_data["fy_start"] == latest_fy].nlargest(top_n, "volume")["oem_name"].tolist()
    fy_data = fy_data[fy_data["oem_name"].isin(top_oems)]
    fy_data = fy_data.sort_values("fy_start")

    return fy_data


def get_oem_fytd_share(category_code, year, month, top_n=7):
    """Get OEM market share for FYTD (Apr to selected month) with YoY comparison.

    Returns DataFrame: oem_name, fytd_vol, fytd_share, prev_fytd_vol, prev_fytd_share, yoy_pct
    """
    fytd_months = get_fytd_months(year, month)
    curr_fytd = _sum_volume_for_months(category_code, fytd_months)
    curr_fytd = curr_fytd.rename(columns={"volume": "fytd_vol"})

    # Previous year FYTD (same months, shifted back 1 year)
    prev_fytd_months = [(y - 1, m) for y, m in fytd_months]
    prev_fytd = _sum_volume_for_months(category_code, prev_fytd_months)
    prev_fytd = prev_fytd.rename(columns={"volume": "prev_fytd_vol"})

    if curr_fytd.empty:
        return pd.DataFrame()

    curr_total = curr_fytd["fytd_vol"].sum()
    curr_fytd["fytd_share"] = (curr_fytd["fytd_vol"] / curr_total * 100).round(1)

    # Top N
    df = curr_fytd.nlargest(top_n, "fytd_vol").copy()
    df = df.merge(prev_fytd, on="oem_name", how="left")

    prev_total = prev_fytd["prev_fytd_vol"].sum() if not prev_fytd.empty else 0
    if prev_total > 0:
        df["prev_fytd_share"] = (df["prev_fytd_vol"] / prev_total * 100).round(1)
    else:
        df["prev_fytd_share"] = None

    df["yoy_pct"] = ((df["fytd_vol"] / df["prev_fytd_vol"]) - 1).mul(100).round(1)

    fy_start = get_fy_start_year(year, month)
    df["fy_label"] = get_fy_label(fy_start)
    df["prev_fy_label"] = get_fy_label(fy_start - 1)

    return df


# ──────────────────────────────────────────────
# OEM STATE MARKET SHARE
# ──────────────────────────────────────────────

def get_oem_state_market_shares(oem_name, category_code, year, month):
    """Get an OEM's market share in each state (volume + share within that state).

    Returns DataFrame: state, oem_volume, state_total, share_pct
    """
    # OEM volumes by state
    oem_state = _query_df("""
        SELECT state, volume as oem_volume
        FROM state_monthly
        WHERE oem_name = ? AND category_code = ? AND year = ? AND month = ?
        ORDER BY volume DESC
    """, [oem_name, category_code, year, month])

    if oem_state.empty:
        return pd.DataFrame()

    # Total volumes by state
    state_totals = _query_df("""
        SELECT state, SUM(volume) as state_total
        FROM state_monthly
        WHERE category_code = ? AND year = ? AND month = ?
        GROUP BY state
    """, [category_code, year, month])

    df = oem_state.merge(state_totals, on="state", how="left")
    df["share_pct"] = (df["oem_volume"] / df["state_total"] * 100).round(1)

    # Also compute contribution % (what % of OEM's total comes from each state)
    oem_total = df["oem_volume"].sum()
    df["contribution_pct"] = (df["oem_volume"] / oem_total * 100).round(1) if oem_total > 0 else 0

    return df.sort_values("oem_volume", ascending=False)


# ──────────────────────────────────────────────
# STATE-LEVEL EV PENETRATION
# ──────────────────────────────────────────────

def get_state_ev_penetration(base_category_code, year, month):
    """Get EV penetration by state for a base category (e.g., 2W → EV_2W/2W).

    Returns DataFrame: state, base_volume, ev_volume, ev_penetration_pct
    """
    ev_code_map = {"2W": "EV_2W", "PV": "EV_PV", "3W": "EV_3W"}
    ev_code = ev_code_map.get(base_category_code)
    if not ev_code:
        return pd.DataFrame()

    # Base category state totals
    base = _query_df("""
        SELECT state, SUM(volume) as base_volume
        FROM state_monthly
        WHERE category_code = ? AND year = ? AND month = ?
        GROUP BY state
    """, [base_category_code, year, month])

    # EV state totals
    ev = _query_df("""
        SELECT state, SUM(volume) as ev_volume
        FROM state_monthly
        WHERE category_code = ? AND year = ? AND month = ?
        GROUP BY state
    """, [ev_code, year, month])

    if base.empty:
        return pd.DataFrame()

    df = base.merge(ev, on="state", how="left")
    df["ev_volume"] = df["ev_volume"].fillna(0)
    df["ev_penetration_pct"] = (df["ev_volume"] / df["base_volume"] * 100).round(2)

    return df.sort_values("ev_penetration_pct", ascending=False)


# ──────────────────────────────────────────────
# FUEL-TYPE AWARE OEM HELPERS
# ──────────────────────────────────────────────

def get_oem_fuel_type_context(oem_name):
    """Determine which category types an OEM participates in.

    Returns dict: {base_category: [list of category_codes]}
    e.g., for Ather: {"2W": ["EV_2W"]}
    e.g., for Tata Motors: {"PV": ["PV", "EV_PV"], "CV": ["CV"]}
    """
    cats = _query_df("""
        SELECT DISTINCT n.category_code, c.is_subsegment, c.base_category_code
        FROM national_monthly n
        JOIN categories c ON n.category_code = c.code
        WHERE n.oem_name = ? AND n.volume > 0
    """, [oem_name])

    result = {}
    for _, row in cats.iterrows():
        code = row["category_code"]
        base = row["base_category_code"] if row["is_subsegment"] else code
        if base not in result:
            result[base] = []
        result[base].append(code)

    return result


def get_relevant_comparison_category(oem_name, base_category):
    """For an OEM in a base category, determine the right comparison universe.

    Since total category (e.g., 2W) includes EV volumes, an EV-only OEM like Ather
    appears in both 2W and EV_2W. We detect pure EV players by comparing their
    volume in the EV subsegment to their total base category volume. If >=80% of
    their volume is EV, we consider them an EV player and compare within EV universe.
    """
    ev_code_map = {"2W": "EV_2W", "PV": "EV_PV", "3W": "EV_3W"}
    ev_code = ev_code_map.get(base_category)
    if not ev_code:
        return base_category

    # Get latest month volumes for comparison
    latest = get_available_months()
    if not latest:
        return base_category
    ly, lm = latest[0]

    # OEM's volume in the base category (total, includes EV)
    base_vol = _query_df("""
        SELECT volume FROM national_monthly
        WHERE oem_name = ? AND category_code = ? AND year = ? AND month = ?
    """, [oem_name, base_category, ly, lm])

    # OEM's volume in the EV subsegment
    ev_vol = _query_df("""
        SELECT volume FROM national_monthly
        WHERE oem_name = ? AND category_code = ? AND year = ? AND month = ?
    """, [oem_name, ev_code, ly, lm])

    base_v = base_vol["volume"].iloc[0] if not base_vol.empty else 0
    ev_v = ev_vol["volume"].iloc[0] if not ev_vol.empty else 0

    # If OEM has EV data but no base category data → pure EV player
    if ev_v > 0 and base_v == 0:
        return ev_code

    # If OEM has EV data and >=80% of base volume is EV → EV player
    if ev_v > 0 and base_v > 0 and (ev_v / base_v) >= 0.80:
        return ev_code

    return base_category


# ──────────────────────────────────────────────
# OEM COMPARISON (MULTI-OEM)
# ──────────────────────────────────────────────

def get_multi_oem_monthly_trend(category_code, oem_names, months=24):
    """Get monthly volume + share for multiple OEMs over last N months."""
    if not oem_names:
        return pd.DataFrame()

    # Get the last N distinct months to filter
    month_df = _query_df("""
        SELECT DISTINCT year, month FROM national_monthly
        WHERE category_code = ? AND volume > 0
        ORDER BY year DESC, month DESC LIMIT ?
    """, [category_code, months])

    if month_df.empty:
        return pd.DataFrame()

    month_conditions = " OR ".join(["(n.year = ? AND n.month = ?)"] * len(month_df))
    month_params = []
    for _, r in month_df.iterrows():
        month_params.extend([int(r["year"]), int(r["month"])])

    placeholders = ",".join(["?"] * len(oem_names))
    df = _query_df(f"""
        SELECT n.oem_name, n.year, n.month, n.volume
        FROM national_monthly n
        WHERE n.category_code = ? AND n.oem_name IN ({placeholders})
              AND ({month_conditions}) AND n.volume > 0
        ORDER BY n.year, n.month
    """, [category_code] + oem_names + month_params)

    if df.empty:
        return df

    # Get category totals
    totals = _query_df("""
        SELECT year, month, SUM(volume) as total_volume
        FROM national_monthly
        WHERE category_code = ?
        GROUP BY year, month
    """, [category_code])

    df = df.merge(totals, on=["year", "month"], how="left")
    df["share_pct"] = (df["volume"] / df["total_volume"] * 100).round(1)
    df["date"] = pd.to_datetime(df[["year", "month"]].assign(day=1))

    return df


def get_top_oems_for_category(category_code, year, month, top_n=5):
    """Get the top N OEM names by volume for a category in a given month."""
    df = _query_df("""
        SELECT oem_name FROM national_monthly
        WHERE category_code = ? AND year = ? AND month = ? AND oem_name != 'Others' AND volume > 0
        ORDER BY volume DESC LIMIT ?
    """, [category_code, year, month, top_n])
    return df["oem_name"].tolist()


# ──────────────────────────────────────────────
# DATA CLEANUP & AGGREGATION
# ──────────────────────────────────────────────

def cleanup_corrupt_state_data():
    """Delete state_monthly rows where oem_name is a serial number (pure digits).

    This fixes data from a parser bug where the Vahan portal's S.No column
    was mistakenly stored as the OEM name.

    Returns number of deleted rows.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        DELETE FROM state_monthly
        WHERE oem_name GLOB '[0-9]*'
          AND LENGTH(oem_name) <= 4
          AND CAST(oem_name AS INTEGER) > 0
    """)
    deleted = cursor.rowcount
    conn.commit()
    conn.close()
    return deleted


def aggregate_state_to_national():
    """Aggregate state_monthly data into national_monthly totals.

    Sums all state volumes for each (category, OEM, year, month) combination
    and upserts into national_monthly with source='vahan_scrape'.

    IMPORTANT: Excludes sentinel category codes (__ALL__, __EV__, __CNG__,
    __HYBRID__) and sentinel OEM names (__TOTAL__) as these are metadata rows
    from the Y-axis scraper, not actual per-OEM per-category data.

    Also excludes categories already covered by Excel data (PV, 2W, 3W, CV,
    TRACTORS and their subsegments) since Excel is the authoritative source
    for national-level data.

    Preserves existing Excel-sourced rows — only inserts new rows or updates
    rows that were previously sourced from scraping.

    Returns number of rows upserted.
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO national_monthly (category_code, oem_name, year, month, volume, source)
        SELECT category_code, oem_name, year, month, SUM(volume), 'vahan_scrape'
        FROM state_monthly
        WHERE volume > 0
          AND category_code NOT IN ('__ALL__', '__EV__', '__CNG__', '__HYBRID__')
          AND oem_name != '__TOTAL__'
        GROUP BY category_code, oem_name, year, month
        ON CONFLICT(category_code, oem_name, year, month)
        DO UPDATE SET volume = excluded.volume,
                      source = excluded.source,
                      updated_at = CURRENT_TIMESTAMP
        WHERE national_monthly.source != 'excel'
    """)
    rows = cursor.rowcount
    conn.commit()
    conn.close()
    return rows




# ── OEM 360 queries (scraped vehcat as single source of truth) ─────────

# Map vehcat category_group -> base app category
VEHCAT_TO_BASE = {
    "PV": "PV", "2W": "2W", "3W": "3W",
    "LCV": "CV", "MHCV": "CV", "BUS": "CV",
    "TRACTOR": "TRACTORS",
    "OTHERS": "OTHERS",
}


def get_oem_total_monthly_scraped(oem_name):
    """Get OEM's total monthly volume from scraped vehcat data.

    Returns DataFrame: year, month, volume (summed across all vehicle categories).
    Uses month > 0 (actual monthly data, not annual aggregates).
    """
    return _query_df("""
        SELECT year, month, SUM(volume) as volume
        FROM national_oem_vehcat
        WHERE oem_name = ? AND month > 0
        GROUP BY year, month
        ORDER BY year, month
    """, [oem_name])


def get_oem_category_breakdown_scraped(oem_name, year, month):
    """Get OEM's volume breakdown by base category for a specific month.

    Returns DataFrame: base_category, volume
    Aggregates vehcat category_group -> base categories (PV, 2W, 3W, CV, TRACTORS).
    """
    df = _query_df("""
        SELECT category_group, SUM(volume) as volume
        FROM national_oem_vehcat
        WHERE oem_name = ? AND year = ? AND month = ?
        GROUP BY category_group
    """, [oem_name, year, month])

    if df.empty:
        return df

    # Map to base categories
    df["base_category"] = df["category_group"].map(VEHCAT_TO_BASE).fillna("OTHERS")
    result = df.groupby("base_category")["volume"].sum().reset_index()
    return result.sort_values("volume", ascending=False)


def get_oem_category_monthly_scraped(oem_name):
    """Get OEM's monthly volume by base category (full time series).

    Returns DataFrame: year, month, base_category, volume
    """
    df = _query_df("""
        SELECT year, month, category_group, SUM(volume) as volume
        FROM national_oem_vehcat
        WHERE oem_name = ? AND month > 0
        GROUP BY year, month, category_group
    """, [oem_name])

    if df.empty:
        return df

    df["base_category"] = df["category_group"].map(VEHCAT_TO_BASE).fillna("OTHERS")
    result = df.groupby(["year", "month", "base_category"])["volume"].sum().reset_index()
    return result.sort_values(["year", "month"])


def get_market_category_monthly_scraped():
    """Get total market volume by base category monthly (all OEMs combined).

    Returns DataFrame: year, month, base_category, volume
    """
    df = _query_df("""
        SELECT year, month, category_group, SUM(volume) as volume
        FROM national_oem_vehcat
        WHERE month > 0
        GROUP BY year, month, category_group
    """)

    if df.empty:
        return df

    df["base_category"] = df["category_group"].map(VEHCAT_TO_BASE).fillna("OTHERS")
    result = df.groupby(["year", "month", "base_category"])["volume"].sum().reset_index()
    return result.sort_values(["year", "month"])


# ── National OEM data queries (from new scraper tables) ──────────────

def get_oem_vehcat_monthly(oem_name, start_year=None, start_month=None):
    """Get OEM's vehicle category breakdown by month.

    Returns DataFrame: year, month, category_group, volume
    Aggregated from raw veh_category codes to consolidated groups.
    """
    query = """
        SELECT year, month, category_group, SUM(volume) as volume
        FROM national_oem_vehcat
        WHERE oem_name = ?
    """
    params = [oem_name]
    if start_year and start_month:
        query += " AND (year > ? OR (year = ? AND month >= ?))"
        params.extend([start_year, start_year, start_month])
    query += " GROUP BY year, month, category_group ORDER BY year, month"
    return _query_df(query, params)


def get_oem_fuel_monthly(oem_name, start_year=None, start_month=None):
    """Get OEM's fuel type breakdown by month.

    Returns DataFrame: year, month, fuel_group, volume
    """
    query = """
        SELECT year, month, fuel_group, SUM(volume) as volume
        FROM national_oem_fuel
        WHERE oem_name = ?
    """
    params = [oem_name]
    if start_year and start_month:
        query += " AND (year > ? OR (year = ? AND month >= ?))"
        params.extend([start_year, start_year, start_month])
    query += " GROUP BY year, month, fuel_group ORDER BY year, month"
    return _query_df(query, params)


def get_oem_vehclass_monthly(oem_name, start_year=None, start_month=None):
    """Get OEM's vehicle class breakdown by month (for Tractor/CE detail).

    Returns DataFrame: year, month, class_group, volume
    """
    query = """
        SELECT year, month, class_group, SUM(volume) as volume
        FROM national_oem_vehclass
        WHERE oem_name = ?
    """
    params = [oem_name]
    if start_year and start_month:
        query += " AND (year > ? OR (year = ? AND month >= ?))"
        params.extend([start_year, start_year, start_month])
    query += " GROUP BY year, month, class_group ORDER BY year, month"
    return _query_df(query, params)


def get_oem_sub_entities(parent_oem):
    """Get sub-entities for a parent OEM from hierarchy table.

    Returns list of dicts: [{oem_raw, sub_brand, business_category, fuel_profile}]
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT oem_raw, sub_brand, business_category, fuel_profile, notes
        FROM oem_hierarchy
        WHERE parent_oem = ? AND sub_brand IS NOT NULL
        ORDER BY sub_brand
    """, (parent_oem,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_oem_sub_brand_volumes(parent_oem, sub_brand, table='national_oem_vehcat',
                               group_col='category_group'):
    """Get volumes for a specific sub-brand within a parent OEM.

    E.g., get Swaraj volumes within Mahindra Tractors.
    Uses oem_raw matching through hierarchy table.
    """
    conn = get_connection()
    raw_names = conn.execute("""
        SELECT oem_raw FROM oem_hierarchy
        WHERE parent_oem = ? AND sub_brand = ?
    """, (parent_oem, sub_brand)).fetchall()
    raw_names = [r['oem_raw'] for r in raw_names]

    if not raw_names:
        conn.close()
        return pd.DataFrame()

    placeholders = ','.join('?' * len(raw_names))
    df = pd.read_sql_query(f"""
        SELECT year, month, {group_col}, SUM(volume) as volume
        FROM {table}
        WHERE oem_raw IN ({placeholders})
        GROUP BY year, month, {group_col}
        ORDER BY year, month
    """, conn, params=raw_names)
    conn.close()
    return df


def get_last_scrape_info():
    """Get latest scrape metadata for display in sidebar.

    Returns list of dicts: [{scrape_type, last_completed, latest_year, latest_month}]
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT scrape_type,
               MAX(completed_at) as last_completed,
               MAX(year * 100 + COALESCE(month, 0)) as ym_key
        FROM scrape_metadata
        WHERE status = 'completed'
        GROUP BY scrape_type
        ORDER BY scrape_type
    """).fetchall()
    conn.close()

    result = []
    for r in rows:
        ym = r['ym_key'] or 0
        result.append({
            'scrape_type': r['scrape_type'],
            'last_completed': r['last_completed'],
            'latest_year': ym // 100 if ym else None,
            'latest_month': ym % 100 if ym else None,
        })
    return result


def get_national_category_totals_from_vehcat(year=None, month=None):
    """Compute national category totals from the vehcat table.

    Can replace Excel-based national_monthly for category totals.
    Returns DataFrame: year, month, category_group, volume
    """
    query = """
        SELECT year, month, category_group, SUM(volume) as volume
        FROM national_oem_vehcat
    """
    params = []
    conditions = []
    if year:
        conditions.append("year = ?")
        params.append(year)
    if month:
        conditions.append("month = ?")
        params.append(month)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " GROUP BY year, month, category_group ORDER BY year, month"
    return _query_df(query, params)


def has_national_oem_data(oem_name=None):
    """Check if we have any data in the national OEM tables."""
    conn = get_connection()
    if oem_name:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM national_oem_vehcat WHERE oem_name = ?",
            (oem_name,)
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) as cnt FROM national_oem_vehcat").fetchone()
    conn.close()
    return row['cnt'] > 0

# ── National OEM Subsegment queries (Selenium-scraped data) ────────────

def get_oem_subsegment_monthly(oem_name, base_category_code):
    """Get an OEM's monthly volumes across all subsegments of a base category.

    Uses the national_oem_subsegment table (Selenium-scraped) cross-referenced
    with the categories table to find which subsegment codes belong to the base.

    Returns DataFrame: year, month, subsegment_code, volume
    Example: get_oem_subsegment_monthly("Tata Motors", "PV")
      -> EV_PV / PV_CNG / PV_HYBRID monthly volumes for Tata
    """
    # Get subsegment codes for this base category
    subs = get_subsegments_for_base(base_category_code)
    if subs.empty:
        return pd.DataFrame(columns=["year", "month", "subsegment_code", "volume"])

    sub_codes = subs["code"].tolist()
    placeholders = ",".join(["?"] * len(sub_codes))
    return _query_df(f"""
        SELECT year, month, subsegment_code, SUM(volume) as volume
        FROM national_oem_subsegment
        WHERE oem_name = ? AND subsegment_code IN ({placeholders})
        GROUP BY year, month, subsegment_code
        ORDER BY year, month, subsegment_code
    """, [oem_name] + sub_codes)


def get_subsegment_market_monthly(subsegment_code):
    """Get market-wide monthly totals for a subsegment (all OEMs combined).

    Used for computing OEM market share within a subsegment.
    Returns DataFrame: year, month, total_volume
    """
    return _query_df("""
        SELECT year, month, SUM(volume) as total_volume
        FROM national_oem_subsegment
        WHERE subsegment_code = ?
        GROUP BY year, month
        ORDER BY year, month
    """, [subsegment_code])


def get_oem_subsegment_summary(oem_name, year, month):
    """Get OEM's subsegment volumes for a specific month (snapshot view).

    Returns DataFrame: subsegment_code, volume
    """
    return _query_df("""
        SELECT subsegment_code, volume
        FROM national_oem_subsegment
        WHERE oem_name = ? AND year = ? AND month = ?
        ORDER BY volume DESC
    """, [oem_name, year, month])


def get_all_oem_subsegment_monthly(subsegment_code):
    """Get all OEMs' monthly volumes for a specific subsegment.

    Used for computing market share and competitive landscape.
    Returns DataFrame: oem_name, year, month, volume
    """
    return _query_df("""
        SELECT oem_name, year, month, volume
        FROM national_oem_subsegment
        WHERE subsegment_code = ? AND volume > 0
        ORDER BY year, month, volume DESC
    """, [subsegment_code])


def has_oem_subsegment_data(oem_name=None):
    """Check if we have any data in national_oem_subsegment table."""
    conn = get_connection()
    if oem_name:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM national_oem_subsegment WHERE oem_name = ?",
            (oem_name,)
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) as cnt FROM national_oem_subsegment").fetchone()
    conn.close()
    return row['cnt'] > 0

# ── Scraped-data query functions for redesigned pages ────────────────

def get_all_categories_monthly_from_vehcat():
    """Get national category totals from scraped vehcat data (replaces Excel-based queries).

    Maps vehcat category_groups to standard categories:
      PV, 2W, 3W -> direct (PV excludes tractor OEMs registered under LMV)
      LCV + MHCV + BUS -> CV
      TRACTORS -> synthetic category from tractor OEMs registered under LMV

    Returns DataFrame: year, month, category_code, volume
    """
    import pandas as pd

    excl_ph, excl_params = _tractor_placeholders()

    # Main categories (PV excludes tractor OEMs)
    main = _query_df(f"""
        SELECT year, month,
               CASE
                   WHEN category_group IN ('LCV','MHCV','BUS') THEN 'CV'
                   ELSE category_group
               END as category_code,
               SUM(volume) as volume
        FROM national_oem_vehcat
        WHERE category_group IN ('PV','2W','3W','LCV','MHCV','BUS')
          AND month BETWEEN 1 AND 12
          AND (category_group != 'PV' OR oem_name NOT IN ({excl_ph}))
        GROUP BY year, month, category_code
        ORDER BY year, month
    """, excl_params)

    # TRACTORS = tractor OEMs that register under LMV
    tractors = _query_df(f"""
        SELECT year, month, 'TRACTORS' as category_code, SUM(volume) as volume
        FROM national_oem_vehcat
        WHERE category_group = 'PV'
          AND month BETWEEN 1 AND 12
          AND oem_name IN ({excl_ph})
        GROUP BY year, month
        ORDER BY year, month
    """, excl_params)

    if tractors.empty:
        return main
    return pd.concat([main, tractors], ignore_index=True)


def get_category_oem_monthly_from_vehcat(category_code):
    """Get per-OEM monthly volumes for a category from scraped vehcat data.

    Handles CV mapping: if category_code='CV', queries LCV+MHCV+BUS.

    Returns DataFrame: year, month, oem_name, volume
    """
    if category_code == 'CV':
        return _query_df("""
            SELECT year, month, oem_name, SUM(volume) as volume
            FROM national_oem_vehcat
            WHERE category_group IN ('LCV','MHCV','BUS')
              AND month BETWEEN 1 AND 12
            GROUP BY year, month, oem_name
            ORDER BY year, month, oem_name
        """)
    if category_code == 'TRACTORS':
        excl_ph, excl_params = _tractor_placeholders()
        return _query_df(f"""
            SELECT year, month, oem_name, SUM(volume) as volume
            FROM national_oem_vehcat
            WHERE category_group = 'PV'
              AND month BETWEEN 1 AND 12
              AND oem_name IN ({excl_ph})
            GROUP BY year, month, oem_name
            ORDER BY year, month, oem_name
        """, excl_params)
    if category_code == 'PV':
        excl_ph, excl_params = _tractor_placeholders()
        return _query_df(f"""
            SELECT year, month, oem_name, SUM(volume) as volume
            FROM national_oem_vehcat
            WHERE category_group = 'PV'
              AND month BETWEEN 1 AND 12
              AND oem_name NOT IN ({excl_ph})
            GROUP BY year, month, oem_name
            ORDER BY year, month, oem_name
        """, excl_params)
    return _query_df("""
        SELECT year, month, oem_name, SUM(volume) as volume
        FROM national_oem_vehcat
        WHERE category_group = ?
          AND month BETWEEN 1 AND 12
        GROUP BY year, month, oem_name
        ORDER BY year, month, oem_name
    """, [category_code])


def get_all_subsegment_totals_monthly():
    """Get market totals per subsegment per month from national_oem_subsegment.

    Returns DataFrame: year, month, subsegment_code, volume
    """
    return _query_df("""
        SELECT year, month, subsegment_code, SUM(volume) as volume
        FROM national_oem_subsegment
        GROUP BY year, month, subsegment_code
        ORDER BY year, month
    """)

# ── Primary Sales queries (OEM dispatches from Excel) ────────────────

def has_primary_data(category=None):
    """Check if primary sales data exists."""
    conn = get_connection()
    if category:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM primary_sales WHERE category = ?", (category,)
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) as cnt FROM primary_sales").fetchone()
    conn.close()
    return row['cnt'] > 0


def get_primary_category_monthly(category):
    """Get monthly total volumes for a category (PV or 2W).

    Returns DataFrame: year, month, volume
    """
    return _query_df("""
        SELECT year, month, SUM(volume) as volume
        FROM primary_sales
        WHERE category = ? AND month BETWEEN 1 AND 12
        GROUP BY year, month
        ORDER BY year, month
    """, [category])


def get_primary_oem_monthly(category):
    """Get per-OEM monthly volumes for a category.

    Returns DataFrame: year, month, oem_name, volume
    """
    return _query_df("""
        SELECT year, month, oem_name, SUM(volume) as volume
        FROM primary_sales
        WHERE category = ? AND month BETWEEN 1 AND 12
        GROUP BY year, month, oem_name
        ORDER BY year, month, oem_name
    """, [category])


def get_primary_segment_monthly(category):
    """Get per-segment monthly volumes for a category.

    Returns DataFrame: year, month, segment, volume
    """
    return _query_df("""
        SELECT year, month, segment, SUM(volume) as volume
        FROM primary_sales
        WHERE category = ? AND month BETWEEN 1 AND 12
        GROUP BY year, month, segment
        ORDER BY year, month
    """, [category])


def get_primary_oem_segment_monthly(category, oem_name):
    """Get an OEM's segment breakdown for a category.

    Returns DataFrame: year, month, segment, volume
    """
    return _query_df("""
        SELECT year, month, segment, SUM(volume) as volume
        FROM primary_sales
        WHERE category = ? AND oem_name = ? AND month BETWEEN 1 AND 12
        GROUP BY year, month, segment
        ORDER BY year, month
    """, [category, oem_name])


def get_primary_latest_month():
    """Get the latest month with primary sales data.

    Returns (year, month) or (None, None).
    """
    conn = get_connection()
    row = conn.execute("""
        SELECT year, month FROM primary_sales
        WHERE month BETWEEN 1 AND 12
        ORDER BY year DESC, month DESC LIMIT 1
    """).fetchone()
    conn.close()
    if row:
        return row['year'], row['month']
    return None, None


def get_primary_import_stats():
    """Get summary stats about imported primary sales data."""
    conn = get_connection()
    row = conn.execute("""
        SELECT COUNT(*) as cnt,
               COUNT(DISTINCT category) as cats,
               COUNT(DISTINCT oem_name) as oems,
               MIN(year || '-' || printf('%02d', month)) as first_month,
               MAX(year || '-' || printf('%02d', month)) as last_month,
               MAX(updated_at) as last_import
        FROM primary_sales
        WHERE month BETWEEN 1 AND 12
    """).fetchone()
    conn.close()
    if row and row['cnt'] > 0:
        return {
            "total_records": row['cnt'],
            "categories": row['cats'],
            "oem_count": row['oems'],
            "first_month": row['first_month'],
            "last_month": row['last_month'],
            "last_import": row['last_import'],
        }
    return None

# ── Additional Primary Sales queries for sub-segment and OEM analysis ──

def get_primary_model_monthly(category, segment):
    """Per-model monthly data for a specific sub-segment.

    Returns DataFrame: year, month, oem_name, model_name, volume
    """
    return _query_df("""
        SELECT year, month, oem_name, model_name, SUM(volume) as volume
        FROM primary_sales
        WHERE category = ? AND segment = ? AND month BETWEEN 1 AND 12
        GROUP BY year, month, oem_name, model_name
        ORDER BY year, month
    """, [category, segment])


def get_primary_oem_in_segment(category, segment):
    """Per-OEM monthly data within a specific sub-segment (aggregated from models).

    Returns DataFrame: year, month, oem_name, volume
    """
    return _query_df("""
        SELECT year, month, oem_name, SUM(volume) as volume
        FROM primary_sales
        WHERE category = ? AND segment = ? AND month BETWEEN 1 AND 12
        GROUP BY year, month, oem_name
        ORDER BY year, month
    """, [category, segment])


def get_primary_oem_total(oem_name, category):
    """Single OEM total monthly volume in a category.

    Returns DataFrame: year, month, volume
    """
    return _query_df("""
        SELECT year, month, SUM(volume) as volume
        FROM primary_sales
        WHERE oem_name = ? AND category = ? AND month BETWEEN 1 AND 12
        GROUP BY year, month
        ORDER BY year, month
    """, [oem_name, category])


def get_primary_oem_segments(oem_name, category):
    """OEM segment breakdown monthly.

    Returns DataFrame: year, month, segment, volume
    """
    return _query_df("""
        SELECT year, month, segment, SUM(volume) as volume
        FROM primary_sales
        WHERE oem_name = ? AND category = ? AND month BETWEEN 1 AND 12
        GROUP BY year, month, segment
        ORDER BY year, month
    """, [oem_name, category])


def get_primary_segments_list(category):
    """List of segments with data for a category.

    Returns list of segment names.
    """
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT segment FROM primary_sales WHERE category = ? ORDER BY segment",
        (category,)
    ).fetchall()
    conn.close()
    return [r['segment'] for r in rows]


def get_primary_oems_list(category):
    """List of OEMs with data for a category, ordered by total volume desc.

    Returns list of OEM names.
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT oem_name, SUM(volume) as total
        FROM primary_sales WHERE category = ? AND month BETWEEN 1 AND 12
        GROUP BY oem_name ORDER BY total DESC
    """, (category,)).fetchall()
    conn.close()
    return [r['oem_name'] for r in rows]

def get_primary_available_months(skip_partial=True):
    """Get all (year, month) pairs with primary sales data, descending.

    If skip_partial=True, the latest month is excluded if its total volume
    is less than 50% of the trailing 3-month average (catches partial imports).

    Returns list of (year, month) tuples.
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT year, month, SUM(volume) as vol
        FROM primary_sales
        WHERE month BETWEEN 1 AND 12
        GROUP BY year, month
        ORDER BY year DESC, month DESC
    """).fetchall()
    conn.close()

    months = [(r['year'], r['month']) for r in rows]

    if skip_partial and len(rows) >= 4:
        latest_vol = rows[0]['vol']
        prior_avg = sum(r['vol'] for r in rows[1:4]) / 3
        if prior_avg > 0 and latest_vol < prior_avg * 0.5:
            months = months[1:]

    return months

def get_vehcat_available_months(skip_partial=True):
    """Get all (year, month) pairs with scraped vehcat data, descending.

    If skip_partial=True (default), the latest month is excluded if its
    total volume is less than 50% of the trailing 3-month average.
    This catches mid-month scrapes that have incomplete data.

    Returns list of (year, month) tuples.
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT year, month, SUM(volume) as vol
        FROM national_oem_vehcat
        WHERE month BETWEEN 1 AND 12
        GROUP BY year, month
        ORDER BY year DESC, month DESC
    """).fetchall()
    conn.close()

    months = [(r['year'], r['month']) for r in rows]

    if skip_partial and len(rows) >= 4:
        latest_vol = rows[0]['vol']
        prior_avg = sum(r['vol'] for r in rows[1:4]) / 3
        if prior_avg > 0 and latest_vol < prior_avg * 0.5:
            # Latest month is likely a partial scrape — skip it
            months = months[1:]

    return months

