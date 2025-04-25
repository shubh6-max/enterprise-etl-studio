# agents/sample_loader_agent.py

from typing import Dict, Any
from utils.snowflake_utils import get_snowflake_connection
import pandas as pd
import json

AGENT_STATE_HINT = {
    "requires": ["resolved_tables"],
    "produces": ["sample_records"]
}

def SampleLoaderAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        resolved_tables = state.get("resolved_tables", {})

        messages = state.get("chatbot_messages", [])
        sample_records = {}

        conn = get_snowflake_connection()
        cursor = conn.cursor()

        try:
            for short_name, fqdn in resolved_tables.items():
                try:
                    query = f"SELECT * FROM {fqdn} LIMIT 5"
                    cursor.execute(query)
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

                    df = pd.DataFrame(rows, columns=columns)
                    sample_records[short_name] = df.to_dict(orient="records")

                    messages.append({
                        "sender": "assistant",
                        "text": f"📊 Here's a preview of `{fqdn}`:\n\n{df.head().to_markdown(index=False)}"
                    })
                except Exception as e:
                    messages.append({
                        "sender": "assistant",
                        "text": f"⚠️ Failed to load sample data from `{fqdn}`: {e}"
                    })
        finally:
            cursor.close()
            conn.close()

        state["sample_records"] = sample_records
        state["chatbot_messages"] = messages

        print("✅ SampleLoaderAgent OUTPUT", json.dumps(sample_records, indent=2, default=str))

        return state

    return invoke
