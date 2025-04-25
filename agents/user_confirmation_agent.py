from typing import Dict, Any

AGENT_STATE_HINT = {
    "requires": ["awaiting_user_confirmation", "chatbot_messages"],
    "produces": ["user_confirmation", "awaiting_user_confirmation"]
}

def UserConfirmationAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("🤖 UserConfirmationAgent invoked")

        messages = state.get("chatbot_messages", [])
        last_user_message = next(
            (m["text"].strip().lower() for m in reversed(messages) if m["sender"] == "user"), None
        )

        print(f"🧾 Last user reply: {last_user_message}")

        # If this is the first time the agent is called, set awaiting_user_confirmation to True
        if not state.get("awaiting_user_confirmation"):
            state["awaiting_user_confirmation"] = True
            messages.append({
                "sender": "assistant",
                "text": "Please reply with **yes** to proceed or rephrase your requirement."
            })
            state["chatbot_messages"] = messages
            return state

        # Handle user confirmation
        if last_user_message == "yes":
            state["user_confirmation"] = "yes"
            state["awaiting_user_confirmation"] = False
            messages.append({
                "sender": "assistant",
                "text": "✅ Thanks! Proceeding with metadata analysis."
            })
            state["chatbot_messages"] = messages
            return state

        # If we're still waiting for confirmation but haven't prompted yet
        if state.get("awaiting_user_confirmation"):
            already_prompted = any(
                "please reply with **yes**" in m["text"].lower() for m in messages if m["sender"] == "assistant"
            )

            if not already_prompted:
                messages.append({
                    "sender": "assistant",
                    "text": "Please reply with **yes** to proceed or rephrase your requirement."
                })
                state["chatbot_messages"] = messages

        return state
    return invoke