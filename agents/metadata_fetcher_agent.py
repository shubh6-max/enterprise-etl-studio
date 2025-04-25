from typing import Dict, Any
from utils.snowflake_utils import describe_table_full
import json

AGENT_STATE_HINT = {
    "requires": ["resolved_tables"],
    "produces": ["raw_metadata"]
}

def MetadataFetcherAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("🟣 MetadataFetcherAgent started (simplified version)!")

        messages = state.get("chatbot_messages", [])
        resolved_tables = state.get("resolved_tables", {})

        # # Remove the last item from resolved_tables (based on insertion order in Python 3.7+)
        # if resolved_tables:
        #     trimmed_tables = dict(list(state["resolved_tables"].items())[:-1])

        # # Optionally, update the state
        # state["resolved_tables"] = trimmed_tables

        raw_metadata = {}

        for short_name, fqdn in resolved_tables.items():
            try:
                print(f"🔍 Describing: {fqdn}")
                describe_result = describe_table_full(fqdn)

                if not describe_result:
                    messages.append({
                        "sender": "assistant",
                        "text": f"❌ Failed to describe `{fqdn}`"
                    })
                    continue

                raw_metadata[short_name] = describe_result
                messages.append({
                    "sender": "assistant",
                    "text": f"📊 Metadata fetched for `{fqdn}`."
                })

            except Exception as e:
                messages.append({
                    "sender": "assistant",
                    "text": f"⚠️ Error fetching metadata for `{fqdn}`: {str(e)}"
                })

        # Fallback if no metadata
        state["raw_metadata"] = raw_metadata or {"_empty": True}

        # 🔁 Ask user for confirmation before proceeding to next step
        state["awaiting_user_confirmation"] = True
        messages.append({
            "sender": "assistant",
            "text": "📋 Metadata has been analyzed. Shall we load sample data next? Please reply with **yes** to continue."
        })

        state["chatbot_messages"] = messages

        print("✅ MetadataFetcherAgent OUTPUT", json.dumps(raw_metadata, indent=2))


        return state

    return invoke
