# agents/clarification_agent.py

from typing import Dict, Any

AGENT_STATE_HINT = {
    "requires": ["clarifications_needed", "ambiguous_tables"],
    "produces": ["clarification_requested", "clarifications_pending"]  # ✅ add both
}

def ClarificationAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("✅ ClarificationAgent")
        ambiguous_tables = state.get("ambiguous_tables", {})
        clarifications_needed = state.get("clarifications_needed", [])
        messages = state.get("chatbot_messages", [])

        clarification_prompt = ""
        for table in clarifications_needed:
            options = ambiguous_tables.get(table, [])
            if options:
                clarification_prompt += f"\n🔹 **{table}** → " + ", ".join([f"`{opt}`" for opt in options])

        if clarification_prompt:
            messages.append({
                "sender": "assistant",
                "text": f"⚠️ I found multiple matches for the following tables:{clarification_prompt}\n\nPlease let me know which one you meant for each."
            })
            state["clarification_requested"] = True
            state["clarifications_pending"] = True  # ✅ Hold planner until clarified
        else:
            state["clarification_requested"] = False
            state["clarifications_pending"] = False

        state["chatbot_messages"] = messages  # ✅ Always update messages
        return state  # ✅ You missed this!
    
    return invoke
