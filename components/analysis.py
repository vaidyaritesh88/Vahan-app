"""Analytical computations: growth rates, FYTD, FY aggregations, period filtering."""
import pandas as pd
import numpy as np


# ── Period Helpers ──

def filter_by_period(df, start_date, end_date, date_col="date"):
    """Filter a DataFrame to rows within [start_date, end_date]."""
    if df.empty:
        return df
    mask = (df[date_col] >= pd.Timestamp(start_date)) & (df[date_col] <= pd.Timestamp(end_date))
    return df[mask].copy()


def get_fy(year, month):
    """Return fiscal year start year. FY26 = Apr 2025 - Mar 2026."""
    return year if month >= 4 else year - 1


def add_fy_columns(df):
    """Add fy (fiscal year start), fy_label, and quarter columns."""
    if df.empty:
        return df
    df = df.copy()
    df["fy"] = df.apply(lambda r: get_fy(int(r["year"]), int(r["month"])), axis=1)
    df["fy_label"] = df["fy"].apply(lambda y: f"FY{str(y + 1)[-2:]}")
    df["quarter"] = df["month"].apply(lambda m: 1 if m in (4, 5, 6) else (2 if m in (7, 8, 9) else (3 if m in (10, 11, 12) else 4)))
    df["q_label"] = df.apply(lambda r: f"{int(r['quarter'])}Q{str(int(r['fy']) + 1)[-2:]}", axis=1)
    return df


# ── Growth Rate Calculations ──

def compute_growth_rates(df, year, month, vol_col="volume"):
    """Compute MoM, QoQ, YoY growth rates for a specific month.

    Returns dict with mom_pct, qoq_pct, yoy_pct (None if data missing).
    """
    if df.empty:
        return {"mom_pct": None, "qoq_pct": None, "yoy_pct": None}

    df = df.copy()
    df["ym"] = df["year"] * 100 + df["month"]
    current = df[df["ym"] == year * 100 + month]
    if current.empty:
        return {"mom_pct": None, "qoq_pct": None, "yoy_pct": None}

    cur_vol = current[vol_col].iloc[0]

    # MoM: previous month
    if month == 1:
        pm_y, pm_m = year - 1, 12
    else:
        pm_y, pm_m = year, month - 1
    prev_month = df[df["ym"] == pm_y * 100 + pm_m]
    mom = _pct_change(cur_vol, prev_month[vol_col].iloc[0]) if not prev_month.empty else None

    # QoQ: same month, previous quarter (3 months ago)
    qm = month - 3
    qy = year
    if qm <= 0:
        qm += 12
        qy -= 1
    prev_q = df[df["ym"] == qy * 100 + qm]
    qoq = _pct_change(cur_vol, prev_q[vol_col].iloc[0]) if not prev_q.empty else None

    # YoY: same month, previous year
    prev_y = df[df["ym"] == (year - 1) * 100 + month]
    yoy = _pct_change(cur_vol, prev_y[vol_col].iloc[0]) if not prev_y.empty else None

    return {"mom_pct": mom, "qoq_pct": qoq, "yoy_pct": yoy}


def _pct_change(current, previous):
    """Compute percentage change, handling zero division."""
    if previous is None or previous == 0:
        return None
    return round(((current / previous) - 1) * 100, 1)


# ── FYTD Calculations ──

def compute_fytd(df, year, month, vol_col="volume"):
    """Compute FYTD volume and FYTD YoY growth.

    FYTD = April of fiscal year through the given month.
    Returns dict with fytd_vol, prev_fytd_vol, fytd_yoy_pct.
    """
    if df.empty:
        return {"fytd_vol": None, "prev_fytd_vol": None, "fytd_yoy_pct": None}

    df = add_fy_columns(df)
    current_fy = get_fy(year, month)

    # Current FYTD: from Apr of current_fy to the given month
    fy_start = pd.Timestamp(current_fy, 4, 1)
    fy_current = pd.Timestamp(year, month, 1)
    mask_current = (df["fy"] == current_fy) & (df["date"] >= fy_start) & (df["date"] <= fy_current)
    fytd_vol = df.loc[mask_current, vol_col].sum()

    # Previous FYTD: same months in previous FY
    prev_fy = current_fy - 1
    prev_start = pd.Timestamp(prev_fy, 4, 1)
    # Corresponding end: same month/day but one year prior
    prev_end_y = year - 1
    prev_end_m = month
    prev_end = pd.Timestamp(prev_end_y, prev_end_m, 1)
    mask_prev = (df["fy"] == prev_fy) & (df["date"] >= prev_start) & (df["date"] <= prev_end)
    prev_fytd_vol = df.loc[mask_prev, vol_col].sum()

    fytd_yoy = _pct_change(fytd_vol, prev_fytd_vol) if prev_fytd_vol > 0 else None

    return {"fytd_vol": fytd_vol, "prev_fytd_vol": prev_fytd_vol, "fytd_yoy_pct": fytd_yoy}


# ── FY over FY (Full Fiscal Year) ──

def compute_fy_volumes(df, vol_col="volume"):
    """Compute full FY volumes. Returns DataFrame with fy_label, volume, yoy_pct."""
    if df.empty:
        return pd.DataFrame()

    df = add_fy_columns(df)
    fy_df = df.groupby(["fy", "fy_label"])[vol_col].sum().reset_index()
    fy_df = fy_df.sort_values("fy")
    fy_df["yoy_pct"] = fy_df[vol_col].pct_change() * 100
    fy_df["yoy_pct"] = fy_df["yoy_pct"].round(1)
    return fy_df


# ── Aggregation by Frequency ──

def aggregate_by_frequency(df, freq="monthly", vol_col="volume", share_col=None):
    """Aggregate data by monthly/quarterly/annual frequency.

    Args:
        df: DataFrame with year, month, date, and vol_col columns.
        freq: 'monthly', 'quarterly', or 'annual'
        vol_col: column to sum
        share_col: if provided, also has oem_volume and total_volume for share recomputation
    """
    if df.empty:
        return df

    df = add_fy_columns(df)

    if freq == "monthly":
        return df

    elif freq == "quarterly":
        group_cols = ["fy", "quarter", "q_label"]
        has_share = "oem_volume" in df.columns and "total_volume" in df.columns
        agg_dict = {vol_col: (vol_col, "sum")}
        if has_share:
            if "oem_volume" != vol_col:
                agg_dict["oem_volume"] = ("oem_volume", "sum")
            agg_dict["total_volume"] = ("total_volume", "sum")
        agg_df = df.groupby(group_cols).agg(**agg_dict).reset_index()
        if has_share:
            oem_col = vol_col if vol_col == "oem_volume" else "oem_volume"
            agg_df["share_pct"] = (agg_df[oem_col] / agg_df["total_volume"] * 100).round(2)
        agg_df["period_label"] = agg_df["q_label"]
        agg_df["date"] = agg_df.apply(
            lambda r: pd.Timestamp(int(r["fy"]) + (1 if r["quarter"] == 4 else 0),
                                   {1: 5, 2: 8, 3: 11, 4: 2}[int(r["quarter"])], 1),
            axis=1
        )
        return agg_df.sort_values("date")

    elif freq == "annual":
        group_cols = ["fy", "fy_label"]
        has_share = "oem_volume" in df.columns and "total_volume" in df.columns
        agg_dict = {vol_col: (vol_col, "sum")}
        if has_share:
            if "oem_volume" != vol_col:
                agg_dict["oem_volume"] = ("oem_volume", "sum")
            agg_dict["total_volume"] = ("total_volume", "sum")
        agg_df = df.groupby(group_cols).agg(**agg_dict).reset_index()
        if has_share:
            oem_col = vol_col if vol_col == "oem_volume" else "oem_volume"
            agg_df["share_pct"] = (agg_df[oem_col] / agg_df["total_volume"] * 100).round(2)
        agg_df["period_label"] = agg_df["fy_label"]
        agg_df["date"] = agg_df["fy"].apply(lambda y: pd.Timestamp(int(y), 10, 1))
        return agg_df.sort_values("date")

    return df


def compute_growth_series(df, vol_col="volume"):
    """Add YoY, QoQ, MoM growth columns to a monthly DataFrame.

    Expects df sorted by date with year, month columns.
    """
    if df.empty:
        return df

    df = df.sort_values(["year", "month"]).copy()
    df["ym"] = df["year"] * 100 + df["month"]

    # Build lookup
    vol_lookup = dict(zip(df["ym"], df[vol_col]))

    def _get_prev_ym(y, m, offset_months):
        for _ in range(offset_months):
            m -= 1
            if m == 0:
                m = 12
                y -= 1
        return y * 100 + m

    yoy, qoq, mom = [], [], []
    for _, row in df.iterrows():
        y, m, v = int(row["year"]), int(row["month"]), row[vol_col]
        prev_y = vol_lookup.get(_get_prev_ym(y, m, 12))
        prev_q = vol_lookup.get(_get_prev_ym(y, m, 3))
        prev_m = vol_lookup.get(_get_prev_ym(y, m, 1))
        yoy.append(_pct_change(v, prev_y))
        qoq.append(_pct_change(v, prev_q))
        mom.append(_pct_change(v, prev_m))

    df["yoy_pct"] = yoy
    df["qoq_pct"] = qoq
    df["mom_pct"] = mom
    return df


# ── Period Preset Helpers ──

PERIOD_PRESETS = {
    "Last 1Y": 12,
    "Last 3Y": 36,
    "Last 5Y": 60,
    "FYTD": "fytd",
    "All Data": None,
}


def get_period_months(preset_name, latest_year, latest_month):
    """Given a preset name and latest available month, return (start_date, end_date)."""
    end_date = pd.Timestamp(latest_year, latest_month, 1)
    n_months = PERIOD_PRESETS.get(preset_name)
    if n_months == "fytd":
        fy_start_year = latest_year if latest_month >= 4 else latest_year - 1
        start_date = pd.Timestamp(fy_start_year, 4, 1)
    elif n_months is None:
        start_date = pd.Timestamp(2000, 1, 1)  # Far enough back to cover all data
    else:
        start_date = end_date - pd.DateOffset(months=n_months - 1)
    return start_date, end_date
