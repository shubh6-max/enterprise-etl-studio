from typing import Dict, Any
from utils.snowflake_utils import get_all_databases_and_schemas
import re

AGENT_STATE_HINT = {
    "requires": [],
    "produces": ["selected_database", "selected_schema"]
}

def DBSchemaSelectorAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("📂 DBSchemaSelectorAgent INVOKED")

        messages = state.get("chatbot_messages", [])
        selected_db = state.get("selected_database")
        selected_schema = state.get("selected_schema")
        user_input = None

        # ✅ Find the most recent user message
        for msg in reversed(messages):
            if msg["sender"] == "user":
                user_input = msg["text"]
                break

        db_schema_map = get_all_databases_and_schemas()
        all_dbs = list(db_schema_map.keys())

        if not selected_db or not selected_schema:
            # ✅ Try extracting DB + schema from user message
            if user_input:
                db_match = re.search(r"(?i)database\s+([a-zA-Z0-9_]+)", user_input)
                schema_match = re.search(r"(?i)schema\s+([a-zA-Z0-9_]+)", user_input)

                if db_match:
                    db = db_match.group(1).upper()
                    if db in db_schema_map:
                        state["selected_database"] = db
                        selected_db = db

                if schema_match and selected_db:
                    schema = schema_match.group(1).upper()
                    if schema in db_schema_map.get(selected_db, []):
                        state["selected_schema"] = schema
                        selected_schema = schema

        # ✅ If both selected, continue
        if selected_db and selected_schema:
            messages.append({
                "sender": "assistant",
                "text": f"📁 Using `{selected_db}` and `{selected_schema}` for table resolution."
            })
            state["awaiting_db_schema"] = False
            state["chatbot_messages"] = messages
            return state

        # 🔁 Otherwise, still waiting for valid input — show options
        db_list = all_dbs[:10]  # limit for simplicity
        display_text = "\n".join([
            f"- {db}: {', '.join(db_schema_map[db][:5]) or 'No schemas'}"
            for db in db_list
        ])

        messages.append({
            "sender": "assistant",
            "text": (
                "📊 Please select the database and schema where your tables are located.\n\n"
                f"Here are some options:\n{display_text}\n\n"
                "👉 Example: `Use database SAMPLE_DB and schema PUBLIC`"
            )
        })

        state["chatbot_messages"] = messages
        state["awaiting_db_schema"] = True
        return state

    return invoke
