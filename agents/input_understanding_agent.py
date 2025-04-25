from typing import Dict, Any
import os
import json
import re
from dotenv import load_dotenv
from utils.llm_provider import model  # Your LLM wrapper (e.g., Groq, Together)

load_dotenv()

AGENT_STATE_HINT = {
    "requires": ["raw_prompt"],
    "produces": ["user_prompt", "mentioned_tables", "output_table_name", "resolved_tables"]
}

def InputUnderstandingAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("🟢 InputUnderstandingAgent invoked")

        prompt = state["raw_prompt"]
        messages = state.get("chatbot_messages", [])

        db = os.getenv("SNOWFLAKE_DATABASE", "SAMPLE_DB")
        schema = os.getenv("SNOWFLAKE_SCHEMA", "SAMPLE_SCHEMA")

        if "resolved_tables" in state and state.get("user_confirmation") == "yes":
            print("⚠️ Skipping InputUnderstandingAgent — already resolved and confirmed")
            return state

        llm_prompt = f"""
You are a Snowflake expert assistant. Extract the following from the business requirement:

1. A clean task summary (as "task")
2. A list of tables mentioned (as "tables")
3. A suitable output table name (as "output_table")
4. Don't include output table name in mentioned_tables

Return ONLY valid JSON in this format:
{{
  "task": "...",
  "tables": ["table1", "table2", ...],
  "output_table": "..."
}}

Business Requirement:
{prompt}
""".strip()

        try:
            print(model)
            response = model.invoke(llm_prompt)
            raw_content = response.content.strip()
            print("🔍 Raw LLM Response:", repr(raw_content))
            cleaned = re.sub(r"^```(?:json)?|```$", "", raw_content, flags=re.MULTILINE).strip()
            refined = json.loads(cleaned)
        except Exception as e:
            print(f"❌ Failed to parse LLM output: {e}")
            refined = {
                "task": prompt.strip(),
                "tables": [],
                "output_table": "fact_result_table"
            }

        user_prompt = refined.get("task", prompt.strip())
        mentioned_tables = refined.get("tables", [])
        output_table = refined.get("output_table", "fact_result_table")

        resolved_tables = {
            table: f"{db}.{schema}.{table}" for table in mentioned_tables
        }

        # ✅ Update extracted values in state
        state["user_prompt"] = user_prompt
        state["mentioned_tables"] = mentioned_tables
        state["output_table_name"] = output_table
        state["resolved_tables"] = resolved_tables
        state["selected_database"] = db
        state["selected_schema"] = schema

        # ✅ Format confirmation message and show in chat
        confirmation_message = f"""
📌 We'll use the following tables from schema `{schema}` in database `{db}`:
{ "\n".join([f"- `{tbl}`" for tbl in mentioned_tables]) or "- (none detected)" }

📋 Output table: `{output_table}`

✅ Shall we proceed to analyze metadata and continue?
👉 Please reply with **yes** to proceed or modify your request.
""".strip()

        messages.append({
            "sender": "assistant",
            "text": confirmation_message
        })

        state["awaiting_user_confirmation"] = True
        state["chatbot_messages"] = messages

        print("✅ InputUnderstandingAgent OUTPUT", json.dumps(state, indent=2))

        return state

    return invoke


