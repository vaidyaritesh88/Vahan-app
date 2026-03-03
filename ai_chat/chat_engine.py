"""Core chat engine: manages Claude API conversations with tool-use loop."""
import json
import anthropic
import plotly.io as pio

from ai_chat.tools import (
    TOOL_DEFINITIONS,
    execute_sql_query,
    create_chart,
    get_data_summary,
)


class ChatEngine:
    """Manages a Claude-powered conversation with tool use for data analysis."""

    MODEL = "claude-sonnet-4-20250514"
    MAX_TOKENS = 4096
    MAX_TOOL_ROUNDS = 5  # prevent infinite tool-use loops

    def __init__(self, api_key: str, db_path: str):
        """Initialize the chat engine.

        Args:
            api_key: Anthropic API key.
            db_path: Path to the SQLite database file.
        """
        self.client = anthropic.Anthropic(api_key=api_key)
        self.db_path = db_path

    def process_message(self, messages: list, system_prompt: str) -> dict:
        """Process the conversation and return the assistant's response.

        Handles iterative tool-use: Claude may call tools, receive results,
        and continue reasoning until it produces a final text response.

        Args:
            messages: List of message dicts with 'role' and 'content' keys.
                      These come from session state and are user/assistant pairs.
            system_prompt: The system prompt with schema and context.

        Returns:
            dict with:
                - content (str): The assistant's text response.
                - charts (list): List of Plotly figure JSON strings.
                - dataframes (list): List of data row lists (for table display).
                - input_tokens (int): Total input tokens used.
                - output_tokens (int): Total output tokens used.
        """
        # Build API messages from session history
        # Only include role + content (strip charts/dataframes which are UI-only)
        api_messages = []
        for msg in messages:
            api_messages.append({
                "role": msg["role"],
                "content": msg["content"],
            })

        charts = []       # Plotly figure JSON strings
        dataframes = []   # List of row-dict lists for table display
        total_input_tokens = 0
        total_output_tokens = 0

        for _round in range(self.MAX_TOOL_ROUNDS):
            response = self.client.messages.create(
                model=self.MODEL,
                max_tokens=self.MAX_TOKENS,
                system=system_prompt,
                tools=TOOL_DEFINITIONS,
                messages=api_messages,
            )

            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens

            if response.stop_reason == "tool_use":
                # Claude wants to use one or more tools
                assistant_content = response.content
                tool_results = []

                for block in assistant_content:
                    if block.type == "tool_use":
                        result = self._execute_tool(block.name, block.input)

                        # Capture chart figures for rendering
                        if block.name == "create_chart" and "figure" in result:
                            fig = result.pop("figure")
                            # Store as JSON for session state serialization
                            charts.append(pio.to_json(fig))
                            result["status"] = "Chart created and displayed successfully."

                        # Capture small dataframes for inline table display
                        if block.name == "execute_sql_query" and "rows" in result:
                            rows = result["rows"]
                            if rows and len(rows) <= 50:
                                dataframes.append(rows)

                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps(result, default=str),
                        })

                # Feed tool results back to Claude for the next round
                api_messages.append({"role": "assistant", "content": assistant_content})
                api_messages.append({"role": "user", "content": tool_results})

            else:
                # Final text response — extract text from content blocks
                text_parts = []
                for block in response.content:
                    if hasattr(block, "text"):
                        text_parts.append(block.text)

                return {
                    "content": "\n".join(text_parts),
                    "charts": charts,
                    "dataframes": dataframes,
                    "input_tokens": total_input_tokens,
                    "output_tokens": total_output_tokens,
                }

        # Hit max rounds — return what we have
        return {
            "content": "I reached the maximum number of analysis steps. Here is what I found so far based on the data queries above.",
            "charts": charts,
            "dataframes": dataframes,
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
        }

    def _execute_tool(self, tool_name: str, tool_input: dict) -> dict:
        """Dispatch and execute a tool call.

        Args:
            tool_name: Name of the tool to execute.
            tool_input: Input parameters for the tool.

        Returns:
            dict with tool results.
        """
        if tool_name == "execute_sql_query":
            return execute_sql_query(tool_input["sql"], self.db_path)
        elif tool_name == "create_chart":
            try:
                fig = create_chart(tool_input)
                return {"figure": fig, "status": "ok"}
            except Exception as e:
                return {"error": f"Chart creation failed: {str(e)}"}
        elif tool_name == "get_data_summary":
            return get_data_summary(self.db_path)
        else:
            return {"error": f"Unknown tool: {tool_name}"}
