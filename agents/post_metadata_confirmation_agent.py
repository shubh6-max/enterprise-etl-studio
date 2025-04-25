from typing import Dict, Any

AGENT_STATE_HINT = {
    "requires": ["raw_metadata", "chatbot_messages"],
    "produces": ["awaiting_sample_confirmation", "user_confirmation"]
}

def PostMetadataConfirmationAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("🤖 PostMetadataConfirmationAgent invoked")

        messages = state.get("chatbot_messages", [])
        last_user_msg = next(
            (m["text"].strip().lower() for m in reversed(messages) if m["sender"] == "user"),
            None
        )

        # Prompt only once
        already_prompted = any(
            "proceed to preview sample data" in m["text"].lower()
            for m in messages if m["sender"] == "assistant"
        )

        if not already_prompted:
            messages.append({
                "sender": "assistant",
                "text": "✅ Metadata extraction complete.\n\n📋 Shall we proceed to preview sample data?\n👉 Please reply with **yes** to continue."
            })
            state["awaiting_sample_confirmation"] = True
            state["user_confirmation"] = None

        # Check for "yes" confirmation
        if last_user_msg == "yes":
            state["awaiting_sample_confirmation"] = False
            state["user_confirmation"] = "yes"
            messages.append({
                "sender": "assistant",
                "text": "✅ Great! Loading sample records now..."
            })

        state["chatbot_messages"] = messages
        return state

    return invoke
