# streamlit_app/run.py

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
from core.state_schema import get_initial_state
from core.langgraph_runner import build_etl_graph
from utils.callbacks import enable_langsmith

# --- Title and layout ---
st.set_page_config(page_title="Enterprise ETL Studio", layout="wide")

# --- Custom CSS ---

# Add custom background
st.markdown("""
    <style>
    .stApp {
        background-color: #f6f2ee;
    }
    </style>
    """, unsafe_allow_html=True)







st.image(
    "https://upload.wikimedia.org/wikipedia/commons/thumb/8/88/MathCo_Logo.png/375px-MathCo_Logo.png",
    width=200  # You can adjust width if needed
)


st.title("AI ETL Orchestrator Studio")

# --- Session state initialization ---
if "chat_state" not in st.session_state:
    st.session_state.chat_state = get_initial_state("")

# --- Chat history rendering ---
for msg in st.session_state.chat_state.get("chatbot_messages", []):
    st.chat_message(msg["sender"]).markdown(msg["text"])

# --- Chat input ---
if user_input := st.chat_input("Describe your data transformation goal or type 'yes' to proceed..."):
    st.chat_message("user").markdown(user_input)
    st.session_state.chat_state["chatbot_messages"].append({
        "sender": "user",
        "text": user_input
    })

    # Only set raw_prompt on first input
    if not st.session_state.chat_state.get("raw_prompt"):
        st.session_state.chat_state["raw_prompt"] = user_input

    # 🧠 Run LangGraph for each user input
    graph = build_etl_graph()
    with enable_langsmith("Enterprise ETL Studio"):
        updated_state = graph.invoke(
            st.session_state.chat_state,
            config={
                "recursion_limit": 20,
                "stop_condition": lambda state: (
                    # Stop if we're waiting for user confirmation
                    state.get("awaiting_user_confirmation") and not state.get("user_confirmation")
                    # Stop if there's a pending message
                    or state.get("pending_confirmation_message") is not None
                    # Stop if workflow is completed
                    or state.get("workflow_status") == "completed"
                    # Stop if we've reached an error state
                    or state.get("error") is not None
                    # Stop if we've been through all major steps
                    or (state.get("metadata_confirmed") 
                        and state.get("sample_confirmed") 
                        and state.get("sql_logic_built")
                        and state.get("sop_validated"))
                )
            }
        )

    # ✅ Step 3: Show assistant message from InputUnderstandingAgent, if any
    if updated_state.get("pending_confirmation_message"):
        updated_state["chatbot_messages"].append({
            "sender": "assistant",
            "text": updated_state["pending_confirmation_message"]
        })
        updated_state["awaiting_user_confirmation"] = True
        updated_state["pending_confirmation_message"] = None

    st.session_state.chat_state = updated_state

