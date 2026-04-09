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


def dual_axis_bar_line(df, x="date", bar_y="volume", line_y="yoy_pct",
                       title="", bar_name="Volume", line_name="YoY %",
                       height=420, yoy_clip=(-80, 120)):
    """Dual-axis chart: bars for volume (left axis) + line for growth % (right axis).

    yoy_clip: (min%, max%) range for the YoY axis. COVID base effects (Apr-Jun 2021
    showing +1000% YoY due to near-zero Apr-Jun 2020) blow up the chart scale.
    Values outside the clip range are capped and shown with a triangle marker.
    Default: -80% to +120% which covers most normal business cycles.
    """
    import numpy as np

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df[x], y=df[bar_y], name=bar_name,
        marker_color="#1f77b4", opacity=0.75,
        yaxis="y",
    ))

    # Clip YoY values for display, mark outliers
    yoy_vals = df[line_y].copy()
    clipped = yoy_vals.clip(lower=yoy_clip[0], upper=yoy_clip[1])
    is_outlier = (yoy_vals < yoy_clip[0]) | (yoy_vals > yoy_clip[1])

    # Main YoY line (clipped values)
    fig.add_trace(go.Scatter(
        x=df[x], y=clipped, name=line_name,
        mode="lines+markers", line=dict(color="#d62728", width=2.5),
        marker=dict(size=6),
        yaxis="y2",
        customdata=yoy_vals,
        hovertemplate="%{customdata:.1f}%<extra></extra>",
    ))

    # Mark outlier points with triangles and actual value annotation
    if is_outlier.any():
        outlier_df = df[is_outlier]
        outlier_clipped = clipped[is_outlier]
        outlier_actual = yoy_vals[is_outlier]
        fig.add_trace(go.Scatter(
            x=outlier_df[x], y=outlier_clipped,
            mode="markers+text",
            marker=dict(symbol="triangle-up", size=12, color="#d62728"),
            text=[f"{v:+.0f}%" for v in outlier_actual],
            textposition="top center",
            textfont=dict(size=9, color="#d62728"),
            showlegend=False,
            yaxis="y2",
            hovertemplate="%{text}<extra>Outlier (clipped)</extra>",
        ))

    fig.update_layout(
        **LAYOUT_DEFAULTS,
        height=height,
        title=title,
        yaxis=dict(title="Volume", side="left"),
        yaxis2=dict(title="YoY %", side="right", overlaying="y",
                    range=[yoy_clip[0] - 10, yoy_clip[1] + 10],
                    zeroline=True, zerolinecolor="rgba(0,0,0,0.2)"),
        hovermode="x unified",
    )
    fig.update_xaxes(title="")
    return fig


def market_share_line_chart(share_df, x_col="label", y_col="share_pct",
                             color_col="oem_name", title="", height=420,
                             colors=None, date_col="date"):
    """Line chart for market share over time.

    Args:
        share_df: DataFrame with x_col, y_col, color_col columns
        colors: Optional list of colors (falls back to OEM_COLORS)
        date_col: Column name for sorting (optional)

    Returns plotly Figure.
    """
    from components.formatters import OEM_COLORS
    palette = colors or OEM_COLORS

    fig = go.Figure()
    groups = share_df[color_col].unique().tolist()
    for i, grp in enumerate(groups):
        d = share_df[share_df[color_col] == grp]
        if date_col in d.columns:
            d = d.sort_values(date_col)
        fig.add_trace(go.Scatter(
            x=d[x_col], y=d[y_col],
            name=str(grp), mode="lines+markers",
            line=dict(width=2, color=palette[i % len(palette)]),
            marker=dict(size=5),
            hovertemplate="%{y:.1f}%<extra>" + str(grp) + "</extra>",
        ))
    layout_kwargs = {k: v for k, v in LAYOUT_DEFAULTS.items() if k != "legend"}
    fig.update_layout(
        **layout_kwargs, height=height, title=title,
        yaxis=dict(title="Share %", ticksuffix="%"),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=-0.3),
    )
    fig.update_xaxes(title="")
    return fig


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
