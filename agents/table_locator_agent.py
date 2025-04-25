from typing import Dict, Any, List
from utils.snowflake_utils import get_snowflake_tables

AGENT_STATE_HINT = {
    "requires": ["mentioned_tables", "selected_database", "selected_schema"],
    "produces": ["resolved_tables", "ambiguous_tables", "clarifications_needed"]
}

def TableLocatorAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("🚀 TableLocatorAgent INVOKED")

        mentioned_tables: List[str] = state.get("mentioned_tables", [])
        db = state.get("selected_database")
        schema = state.get("selected_schema")

        resolved_tables = {}
        ambiguous_tables = {}
        clarifications_needed = []

        if not db or not schema:
            raise ValueError("❌ selected_database or selected_schema missing in state")

        available_tables = get_snowflake_tables(db, schema)
        print(f"📦 Tables in {db}.{schema} → {available_tables}")

        for table_name in mentioned_tables:
            matches = [t for t in available_tables if t.lower() == table_name.lower()]
            if len(matches) == 1:
                fqdn = f"{db}.{schema}.{matches[0]}"
                resolved_tables[table_name] = fqdn
            elif len(matches) > 1:
                ambiguous_tables[table_name] = [f"{db}.{schema}.{m}" for m in matches]
                clarifications_needed.append(table_name)
            else:
                # Not found, still add to clarification
                ambiguous_tables[table_name] = []
                clarifications_needed.append(table_name)

        print("✅ resolved_tables =", resolved_tables)
        print("⚠️ ambiguous_tables =", ambiguous_tables)
        print("❓ clarifications_needed =", clarifications_needed)

        state["resolved_tables"] = resolved_tables
        state["ambiguous_tables"] = ambiguous_tables
        state["clarifications_needed"] = clarifications_needed

        # Optionally log to chatbot
        if resolved_tables:
            resolved_msg = "\n".join([f"- `{k}` → `{v}`" for k, v in resolved_tables.items()])
            state["chatbot_messages"].append({
                "sender": "assistant",
                "text": f"🔍 Located the following tables in `{db}.{schema}`:\n{resolved_msg}"
            })

        if clarifications_needed:
            clarify_msg = "\n".join([f"- `{tbl}` → {ambiguous_tables.get(tbl, [])}" for tbl in clarifications_needed])
            state["chatbot_messages"].append({
                "sender": "assistant",
                "text": f"⚠️ I couldn't resolve or found multiple matches for these tables:\n{clarify_msg}\n\nPlease clarify."
            })

        return state

    return invoke
