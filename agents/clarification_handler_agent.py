# agents/clarification_handler_agent.py

from typing import Dict, Any
import re

AGENT_STATE_HINT = {
    "requires": ["clarifications_needed", "ambiguous_tables", "chatbot_messages"],
    "produces": ["resolved_tables", "clarifications_needed", "clarifications_pending"]  # ✅ Added
}

def ClarificationHandlerAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("✅ ClarificationHandlerAgent")
        resolved = state.get("resolved_tables", {})
        ambiguous = state.get("ambiguous_tables", {})
        needed = state.get("clarifications_needed", [])
        messages = state.get("chatbot_messages", [])

        if not messages or not needed:
            state["clarifications_pending"] = False  # Still needed
            return state

        # 🔍 Find the latest user message after the last clarification request
        clarify_index = max(
            idx for idx, m in enumerate(messages) if m["sender"] == "assistant" and "multiple matches" in m["text"]
        )
        user_msg = next(
            (m["text"] for m in messages[clarify_index + 1:] if m["sender"] == "user"),
            None
        )

        # 🧯 If no new user reply yet, don’t process — just wait
        if not user_msg:
            state["clarifications_pending"] = True  # 👈 This was missing
            return state


        updated_needed = []

        for table in needed:
            options = ambiguous.get(table, [])
            matched = None

            for opt in options:
                if opt.lower() in user_msg.lower() or re.search(table, user_msg, re.IGNORECASE):
                    matched = opt
                    break

            if matched:
                resolved[table] = matched
            else:
                updated_needed.append(table)

        state["resolved_tables"] = resolved
        state["clarifications_needed"] = updated_needed
        state["clarifications_pending"] = bool(updated_needed)  # ✅ Set correctly

        if not updated_needed:
            messages.append({
                "sender": "assistant",
                "text": "✅ Thanks for the clarification. All table ambiguities are now resolved!"
            })
        else:
            messages.append({
                "sender": "assistant",
                "text": f"⚠️ Still need clarification for: {', '.join(updated_needed)}"
            })

        state["chatbot_messages"] = messages
        return state

    return invoke
