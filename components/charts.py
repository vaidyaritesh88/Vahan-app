"""Reusable Plotly chart builders."""
import plotly.express as px
import plotly.graph_objects as go
from components.formatters import format_units, OEM_COLORS


LAYOUT_DEFAULTS = dict(
    template="plotly_white",
    margin=dict(l=40, r=20, t=40, b=40),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    font=dict(size=12),
)


def monthly_bar_chart(df, x="date", y="volume", color=None, title="", barmode="group", height=420):
    """Standard monthly bar chart."""
    fig = px.bar(df, x=x, y=y, color=color, barmode=barmode, title=title,
                 color_discrete_sequence=OEM_COLORS)
    fig.update_layout(**LAYOUT_DEFAULTS, height=height)
    fig.update_xaxes(title="")
    fig.update_yaxes(title="Units")
    return fig


def stacked_bar_chart(df, x="date", y="volume", color="oem_name", title="", height=420):
    """Stacked bar chart for composition analysis."""
    fig = px.bar(df, x=x, y=y, color=color, title=title, barmode="stack",
                 color_discrete_sequence=OEM_COLORS)
    fig.update_layout(**LAYOUT_DEFAULTS, height=height)
    fig.update_xaxes(title="")
    return fig


def line_chart(df, x="date", y="value", color=None, title="", height=400, markers=True):
    """Multi-line trend chart."""
    fig = px.line(df, x=x, y=y, color=color, title=title, markers=markers,
                  color_discrete_sequence=OEM_COLORS)
    fig.update_layout(**LAYOUT_DEFAULTS, height=height)
    fig.update_xaxes(title="")
    return fig


def donut_chart(df, names="oem_name", values="volume", title="", height=400):
    """Donut/pie chart for market share."""
    fig = px.pie(df, names=names, values=values, hole=0.45, title=title,
                 color_discrete_sequence=OEM_COLORS)
    fig.update_traces(textposition="inside", textinfo="percent+label",
                      textfont_size=11)
    fig.update_layout(height=height, margin=dict(l=20, r=20, t=40, b=20),
                      showlegend=False)
    return fig


def horizontal_bar(df, x="volume", y="oem_name", title="", height=None, text=None):
    """Horizontal bar chart, sorted by value."""
    h = height or max(300, len(df) * 35)
    fig = px.bar(df, x=x, y=y, orientation="h", title=title, text=text,
                 color_discrete_sequence=["#1f77b4"])
    fig.update_layout(**LAYOUT_DEFAULTS, height=h)
    fig.update_yaxes(categoryorder="total ascending", title="")
    fig.update_xaxes(title="")
    return fig


def kpi_row(metrics, cols):
    """Render a row of KPI metrics using Streamlit columns.

    Args:
        metrics: list of (label, value, delta) tuples
        cols: Streamlit columns objects
    """
    for col, (label, value, delta) in zip(cols, metrics):
        with col:
            import streamlit as st
            st.metric(
                label=label,
                value=format_units(value) if isinstance(value, (int, float)) else str(value),
                delta=f"{delta:+.1f}%" if isinstance(delta, (int, float)) else delta,
            )


def yoy_bar_chart(df, title="YoY Growth", height=350):
    """Bar chart showing YoY growth rates with color coding."""
    fig = go.Figure()
    colors = ["#2ca02c" if v >= 0 else "#d62728" for v in df["yoy_pct"]]
    fig.add_trace(go.Bar(
        x=df["category_name"],
        y=df["yoy_pct"],
        marker_color=colors,
        text=[f"{v:+.1f}%" if v is not None else "N/A" for v in df["yoy_pct"]],
        textposition="outside",
    ))
    fig.update_layout(**LAYOUT_DEFAULTS, height=height, title=title)
    fig.update_yaxes(title="YoY %")
    fig.update_xaxes(title="")
    return fig
