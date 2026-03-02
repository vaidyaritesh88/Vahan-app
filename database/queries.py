"""All database query functions used by the Streamlit pages."""
import pandas as pd
from database.schema import get_connection


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
        GROUP BY state
        ORDER BY volume DESC
    """, [category_code, year, month])


def get_state_oem_breakdown(category_code, state, year, month, top_n=10):
    """Get OEM breakdown within a specific state for a category."""
    df = _query_df("""
        SELECT oem_name, volume
        FROM state_monthly
        WHERE category_code = ? AND state = ? AND year = ? AND month = ? AND volume > 0
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
