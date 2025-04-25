from typing import Dict, Any

AGENT_STATE_HINT = {
    "requires": ["sample_records", "chatbot_messages"],
    "produces": ["user_confirmation_after_sample", "awaiting_sample_confirmation"]
}

def PostSampleConfirmationAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("🤖 PostSampleConfirmationAgent invoked")

        messages = state.get("chatbot_messages", [])
        last_user_msg = next(
            (m["text"].strip().lower() for m in reversed(messages) if m["sender"] == "user"),
            None
        )

        # Prompt only once
        already_prompted = any(
            "proceed to sql generation" in m["text"].lower()
            for m in messages if m["sender"] == "assistant"
        )

        if not already_prompted:
            messages.append({
                "sender": "assistant",
                "text": "👀 You've now seen sample data from input tables.\n\n📋 Shall we proceed to generate SQL logic?\n👉 Please reply with **yes** to continue."
            })
            state["awaiting_sample_confirmation"] = True
            state["user_confirmation_after_sample"] = None

        # Check for "yes" confirmation
        if last_user_msg == "yes":
            state["awaiting_sample_confirmation"] = False
            state["user_confirmation_after_sample"] = "yes"
            messages.append({
                "sender": "assistant",
                "text": "✅ Thanks! Proceeding to generate SQL logic from your requirement."
            })

        state["chatbot_messages"] = messages
        return state

    return invoke
