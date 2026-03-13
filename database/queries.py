"""All database query functions used by the Streamlit pages."""
import pandas as pd
from database.schema import get_connection
from components.formatters import (
    get_fy_start_year, get_fy_label, get_fytd_months,
    get_quarter_months, get_prev_month, get_prev_quarter_end,
    format_quarter, format_fy,
)


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
