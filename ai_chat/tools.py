"""Tool definitions and handlers for Claude API tool use."""
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from components.charts import LAYOUT_DEFAULTS
from components.formatters import OEM_COLORS


# ── Tool Definitions (Claude API format) ──

TOOL_DEFINITIONS = [
    {
        "name": "execute_sql_query",
        "description": (
            "Execute a read-only SQL query against the vehicle registration SQLite database. "
            "Returns results as a JSON array of row objects. Use this to answer any data question. "
            "Only SELECT statements are allowed. The database uses SQLite syntax."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A SELECT SQL query to execute against the SQLite database.",
                },
                "description": {
                    "type": "string",
                    "description": "A brief human-readable description of what this query does.",
                },
            },
            "required": ["sql", "description"],
        },
    },
    {
        "name": "create_chart",
        "description": (
            "Create a Plotly chart from data. Provide the chart type, data rows, and configuration. "
            "The chart will be rendered inline in the chat. Use this after querying data to visualize results."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "chart_type": {
                    "type": "string",
                    "enum": ["bar", "line", "horizontal_bar", "donut", "stacked_bar", "grouped_bar", "area"],
                    "description": "Type of chart to create.",
                },
                "data": {
                    "type": "array",
                    "items": {"type": "object"},
                    "description": "Array of data objects (rows) for the chart. Each object is a row with column name keys.",
                },
                "x": {
                    "type": "string",
                    "description": "Column name for x-axis (or 'names' for donut chart).",
                },
                "y": {
                    "type": "string",
                    "description": "Column name for y-axis (or 'values' for donut chart).",
                },
                "color": {
                    "type": "string",
                    "description": "Optional column name for color grouping (creates multiple series).",
                },
                "title": {
                    "type": "string",
                    "description": "Chart title.",
                },
                "x_label": {
                    "type": "string",
                    "description": "X-axis label (optional).",
                },
                "y_label": {
                    "type": "string",
                    "description": "Y-axis label (optional).",
                },
                "height": {
                    "type": "integer",
                    "description": "Chart height in pixels (optional, default 450).",
                },
            },
            "required": ["chart_type", "data", "x", "y", "title"],
        },
    },
    {
        "name": "get_data_summary",
        "description": (
            "Get a summary of available data in the database: categories with date ranges, "
            "top OEMs by volume, states with data, and record counts. "
            "Use this when you need to understand what data is available before querying."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]


# ── Tool Handlers ──

def execute_sql_query(sql: str, db_path: str, timeout: int = 10) -> dict:
    """Execute a read-only SQL query with safety checks.

    Returns dict with 'rows' (list of dicts), 'total_rows', 'truncated', or 'error'.
    """
    # Safety: reject non-SELECT statements
    cleaned = sql.strip()
    # Remove leading comments
    while cleaned.startswith("--"):
        cleaned = cleaned.split("\n", 1)[-1].strip() if "\n" in cleaned else ""
    cleaned_upper = cleaned.upper()

    if not cleaned_upper.startswith("SELECT") and not cleaned_upper.startswith("WITH"):
        return {"error": "Only SELECT statements (and WITH/CTE) are allowed.", "rows": []}

    # Check for forbidden keywords outside string literals
    forbidden_keywords = [
        "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE",
        "ATTACH", "DETACH", "REPLACE", "VACUUM", "REINDEX",
    ]
    # Simple check: split by single quotes to isolate non-string parts
    parts = cleaned_upper.split("'")
    non_string_parts = " ".join(parts[0::2])  # even-indexed parts are outside strings
    for keyword in forbidden_keywords:
        # Check as whole word (preceded/followed by space, start/end, or punctuation)
        import re
        if re.search(rf'\b{keyword}\b', non_string_parts):
            return {"error": f"Statement contains forbidden keyword: {keyword}", "rows": []}

    try:
        conn = sqlite3.connect(db_path, timeout=timeout)
        conn.execute("PRAGMA query_only = ON")
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()

        # Limit result size to avoid token explosion
        max_rows = 200
        if len(rows) > max_rows:
            return {
                "rows": rows[:max_rows],
                "total_rows": len(rows),
                "truncated": True,
                "message": f"Query returned {len(rows)} rows. Showing first {max_rows}. Consider adding LIMIT or more specific filters.",
            }
        return {"rows": rows, "total_rows": len(rows), "truncated": False}

    except sqlite3.OperationalError as e:
        return {"error": f"SQL error: {str(e)}", "rows": []}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}", "rows": []}


def create_chart(chart_spec: dict) -> go.Figure:
    """Build a Plotly figure from a chart specification.

    Args:
        chart_spec: dict with chart_type, data, x, y, and optional color, title, etc.

    Returns:
        plotly Figure object.
    """
    df = pd.DataFrame(chart_spec["data"])
    chart_type = chart_spec["chart_type"]
    x = chart_spec["x"]
    y = chart_spec["y"]
    color = chart_spec.get("color")
    title = chart_spec.get("title", "")
    height = chart_spec.get("height", 450)

    if df.empty:
        fig = go.Figure()
        fig.update_layout(
            title=title,
            annotations=[dict(text="No data available", x=0.5, y=0.5,
                              xref="paper", yref="paper", showarrow=False, font=dict(size=16))]
        )
        return fig

    if chart_type == "bar":
        fig = px.bar(df, x=x, y=y, color=color, title=title, barmode="group",
                     color_discrete_sequence=OEM_COLORS)
    elif chart_type == "line":
        fig = px.line(df, x=x, y=y, color=color, title=title, markers=True,
                      color_discrete_sequence=OEM_COLORS)
    elif chart_type == "horizontal_bar":
        fig = px.bar(df, x=y, y=x, orientation="h", title=title, color=color,
                     color_discrete_sequence=["#1f77b4"] if not color else OEM_COLORS)
        fig.update_yaxes(categoryorder="total ascending")
    elif chart_type == "donut":
        fig = px.pie(df, names=x, values=y, hole=0.45, title=title,
                     color_discrete_sequence=OEM_COLORS)
        fig.update_traces(textposition="inside", textinfo="percent+label", textfont_size=11)
    elif chart_type == "stacked_bar":
        fig = px.bar(df, x=x, y=y, color=color, barmode="stack", title=title,
                     color_discrete_sequence=OEM_COLORS)
    elif chart_type == "grouped_bar":
        fig = px.bar(df, x=x, y=y, color=color, barmode="group", title=title,
                     color_discrete_sequence=OEM_COLORS)
    elif chart_type == "area":
        fig = px.area(df, x=x, y=y, color=color, title=title,
                      color_discrete_sequence=OEM_COLORS)
    else:
        fig = px.bar(df, x=x, y=y, color=color, title=title,
                     color_discrete_sequence=OEM_COLORS)

    fig.update_layout(**LAYOUT_DEFAULTS, height=height)

    if chart_spec.get("x_label"):
        fig.update_xaxes(title=chart_spec["x_label"])
    if chart_spec.get("y_label"):
        fig.update_yaxes(title=chart_spec["y_label"])

    return fig


def get_data_summary(db_path: str) -> dict:
    """Return a structured summary of available data in the database."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Category-level freshness from national data
        freshness_df = pd.read_sql_query("""
            SELECT category_code,
                   MIN(year * 100 + month) as earliest_ym,
                   MAX(year * 100 + month) as latest_ym,
                   COUNT(DISTINCT oem_name) as oem_count,
                   COUNT(*) as record_count,
                   CAST(SUM(volume) AS INTEGER) as total_volume
            FROM national_monthly
            WHERE volume > 0
            GROUP BY category_code
            ORDER BY category_code
        """, conn)

        # Top OEMs by total volume
        top_oems_df = pd.read_sql_query("""
            SELECT oem_name, CAST(SUM(volume) AS INTEGER) as total_vol
            FROM national_monthly
            WHERE volume > 0 AND oem_name != 'Others'
            GROUP BY oem_name
            ORDER BY total_vol DESC
            LIMIT 30
        """, conn)

        # States with data
        states_df = pd.read_sql_query("""
            SELECT DISTINCT state FROM state_monthly WHERE volume > 0 ORDER BY state
        """, conn)

        # State data freshness
        state_freshness_df = pd.read_sql_query("""
            SELECT category_code,
                   COUNT(DISTINCT state) as state_count,
                   MAX(year * 100 + month) as latest_ym
            FROM state_monthly
            WHERE volume > 0
            GROUP BY category_code
        """, conn)

        conn.close()

        return {
            "category_freshness": freshness_df.to_dict("records"),
            "top_oems": top_oems_df["oem_name"].tolist(),
            "states_with_data": states_df["state"].tolist() if not states_df.empty else [],
            "state_data_freshness": state_freshness_df.to_dict("records") if not state_freshness_df.empty else [],
            "total_national_records": int(freshness_df["record_count"].sum()) if not freshness_df.empty else 0,
        }

    except Exception as e:
        return {"error": f"Failed to get data summary: {str(e)}"}
