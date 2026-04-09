"""Page 8: AI Chat - Ask natural language questions about vehicle registration data."""
import streamlit as st
import pandas as pd
import plotly.io as pio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.schema import init_db
from database.queries import get_latest_month, get_record_counts
from components.formatters import format_month, format_units
from config.settings import DB_PATH
from ai_chat.chat_engine import ChatEngine
from ai_chat.system_prompt import build_system_prompt
from ai_chat.export import export_chat_summary, export_saved_items

init_db()

st.title("💬 AI Chat — Ask About Your Data")
st.caption("Ask natural language questions about vehicle registration data. Get answers, charts, and calculations powered by Claude.")

# ── Session State Initialization ──

if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []

if "saved_responses" not in st.session_state:
    st.session_state.saved_responses = []

if "token_usage" not in st.session_state:
    st.session_state.token_usage = {"input": 0, "output": 0}


# ── Sidebar: API Key ──

st.sidebar.title("AI Chat")

api_key = None
# 1. Check Streamlit secrets
try:
    api_key = st.secrets["ANTHROPIC_API_KEY"]
except (KeyError, FileNotFoundError):
    pass

# 2. Check environment variable
if not api_key:
    api_key = os.environ.get("ANTHROPIC_API_KEY")

# 3. Manual input fallback
if not api_key:
    st.sidebar.markdown("**Enter your Anthropic API key:**")
    api_key = st.sidebar.text_input(
        "API Key",
        type="password",
        key="anthropic_api_key_input",
        help="Get your key at console.anthropic.com",
        label_visibility="collapsed",
    )

if not api_key:
    st.info(
        "**To get started**, enter your Anthropic API key in the sidebar.\n\n"
        "You can get one at [console.anthropic.com](https://console.anthropic.com/).\n\n"
        "Alternatively, add it to `.streamlit/secrets.toml`:\n"
        "```toml\nANTHROPIC_API_KEY = \"sk-ant-...\"\n```"
    )
    st.stop()


# ── Sidebar: Data Context ──

st.sidebar.divider()
st.sidebar.subheader("📊 Data Context")

try:
    ly, lm = get_latest_month()
    counts = get_record_counts()
    st.sidebar.markdown(f"**Latest data:** {format_month(ly, lm)}")
    st.sidebar.markdown(f"**National records:** {counts['national_monthly']:,}")
    st.sidebar.markdown(f"**State records:** {counts['state_monthly']:,}")
except Exception:
    ly, lm = 2026, 1
    counts = {"national_monthly": 0, "state_monthly": 0, "weekly_trends": 0}
    st.sidebar.warning("Could not load data context.")


# ── Sidebar: Chat Actions ──

st.sidebar.divider()
st.sidebar.subheader("💾 Actions")

if st.sidebar.button("🗑️ Clear Chat", use_container_width=True):
    st.session_state.chat_messages = []
    st.session_state.token_usage = {"input": 0, "output": 0}
    st.rerun()

# Export chat summary
if st.session_state.chat_messages:
    summary_md = export_chat_summary(st.session_state.chat_messages)
    st.sidebar.download_button(
        "📥 Export Chat Summary",
        data=summary_md,
        file_name="vahan_chat_summary.md",
        mime="text/markdown",
        use_container_width=True,
    )

# Saved responses
if st.session_state.saved_responses:
    st.sidebar.markdown(f"**Saved insights:** {len(st.session_state.saved_responses)}")
    saved_md = export_saved_items(st.session_state.saved_responses)
    st.sidebar.download_button(
        "📥 Export Saved Insights",
        data=saved_md,
        file_name="vahan_saved_insights.md",
        mime="text/markdown",
        use_container_width=True,
    )
    if st.sidebar.button("Clear Saved Insights", use_container_width=True):
        st.session_state.saved_responses = []
        st.rerun()

# Token usage
st.sidebar.divider()
st.sidebar.caption(
    f"Tokens: {st.session_state.token_usage['input']:,} in / "
    f"{st.session_state.token_usage['output']:,} out"
)


# ── Sidebar: Example Questions ──

st.sidebar.divider()
st.sidebar.subheader("💡 Example Questions")
example_questions = [
    "What is the total PV volume for the latest month?",
    "Show top 5 2W OEMs by market share as a bar chart",
    "Compare Tata Motors vs Maruti Suzuki in PV for the last 6 months",
    "What is the EV penetration trend for PV?",
    "Which state has the highest 2W registrations?",
    "Show YoY growth for all main categories",
]
for q in example_questions:
    st.sidebar.caption(f"• {q}")


# ── Chat Message Display ──

for i, msg in enumerate(st.session_state.chat_messages):
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

        # Render charts attached to this message
        if "charts" in msg:
            for chart_json in msg["charts"]:
                try:
                    fig = pio.from_json(chart_json)
                    st.plotly_chart(fig, use_container_width=True)
                except Exception:
                    st.warning("Could not render chart.")

        # Render data tables
        if "dataframes" in msg:
            for df_data in msg["dataframes"]:
                try:
                    df = pd.DataFrame(df_data)
                    if not df.empty:
                        st.dataframe(df, use_container_width=True, hide_index=True)
                except Exception:
                    pass

        # Save button for assistant messages
        if msg["role"] == "assistant":
            if st.button("💾 Save this insight", key=f"save_{i}"):
                # Avoid duplicates
                if msg not in st.session_state.saved_responses:
                    st.session_state.saved_responses.append(msg)
                    st.toast("✅ Response saved!")
                else:
                    st.toast("Already saved.")


# ── Chat Input ──

if prompt := st.chat_input("Ask about vehicle registrations..."):
    # Display user message
    st.session_state.chat_messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Process with Claude
    with st.chat_message("assistant"):
        with st.spinner("Analyzing your data..."):
            try:
                engine = ChatEngine(api_key, DB_PATH)
                system_prompt = build_system_prompt(ly, lm, counts)
                response = engine.process_message(
                    st.session_state.chat_messages,
                    system_prompt,
                )

                # Display text response
                st.markdown(response["content"])

                # Display charts
                rendered_charts = []
                for chart_json in response.get("charts", []):
                    try:
                        fig = pio.from_json(chart_json)
                        st.plotly_chart(fig, use_container_width=True)
                        rendered_charts.append(chart_json)
                    except Exception:
                        st.warning("Could not render a chart.")

                # Display data tables
                rendered_dfs = []
                for df_data in response.get("dataframes", []):
                    try:
                        df = pd.DataFrame(df_data)
                        if not df.empty:
                            st.dataframe(df, use_container_width=True, hide_index=True)
                            rendered_dfs.append(df_data)
                    except Exception:
                        pass

                # Store assistant message in session state
                assistant_msg = {"role": "assistant", "content": response["content"]}
                if rendered_charts:
                    assistant_msg["charts"] = rendered_charts
                if rendered_dfs:
                    assistant_msg["dataframes"] = rendered_dfs
                st.session_state.chat_messages.append(assistant_msg)

                # Update token usage
                st.session_state.token_usage["input"] += response.get("input_tokens", 0)
                st.session_state.token_usage["output"] += response.get("output_tokens", 0)

            except Exception as e:
                error_str = str(e)
                if "authentication" in error_str.lower() or "api key" in error_str.lower():
                    st.error("❌ Invalid Anthropic API key. Please check and try again.")
                elif "rate" in error_str.lower() and "limit" in error_str.lower():
                    st.warning("⏳ Rate limited by the API. Please wait a moment and try again.")
                elif "connection" in error_str.lower():
                    st.error("🌐 Cannot connect to Anthropic API. Check your internet connection.")
                else:
                    st.error(f"An error occurred: {error_str}")

    # Force rerun to update sidebar token count and show save buttons
    st.rerun()
