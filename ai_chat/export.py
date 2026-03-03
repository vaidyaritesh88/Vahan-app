"""Export utilities for chat summaries and saved responses."""
import pandas as pd
from datetime import datetime


def export_chat_summary(messages: list) -> str:
    """Export the full chat conversation as a Markdown document.

    Args:
        messages: List of message dicts from session state.
                  Each has 'role', 'content', and optionally 'dataframes'.

    Returns:
        Markdown-formatted string.
    """
    lines = [
        "# Vahan Tracker - AI Chat Summary",
        "",
        f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
        "",
        "---",
        "",
    ]

    for msg in messages:
        role = "**You**" if msg["role"] == "user" else "**AI Analyst**"
        lines.append(f"### {role}")
        lines.append("")
        lines.append(msg["content"])
        lines.append("")

        # Include data tables if present
        if "dataframes" in msg and msg["dataframes"]:
            for df_data in msg["dataframes"]:
                try:
                    df = pd.DataFrame(df_data)
                    if not df.empty:
                        lines.append(df.to_markdown(index=False))
                        lines.append("")
                except Exception:
                    pass

        # Note charts (can't embed in markdown)
        if "charts" in msg and msg["charts"]:
            lines.append(f"*[{len(msg['charts'])} chart(s) rendered in application]*")
            lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def export_saved_items(saved: list) -> str:
    """Export only saved responses as a Markdown document.

    Args:
        saved: List of saved assistant message dicts.

    Returns:
        Markdown-formatted string.
    """
    lines = [
        "# Vahan Tracker - Saved Insights",
        "",
        f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
        "",
        "---",
        "",
    ]

    for i, msg in enumerate(saved, 1):
        lines.append(f"## Insight {i}")
        lines.append("")
        lines.append(msg.get("content", ""))
        lines.append("")

        # Include data tables
        if "dataframes" in msg and msg["dataframes"]:
            for df_data in msg["dataframes"]:
                try:
                    df = pd.DataFrame(df_data)
                    if not df.empty:
                        lines.append(df.to_markdown(index=False))
                        lines.append("")
                except Exception:
                    pass

        if "charts" in msg and msg["charts"]:
            lines.append(f"*[{len(msg['charts'])} chart(s) rendered in application]*")
            lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines)
