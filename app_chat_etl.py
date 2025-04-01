import streamlit as st
from llm.chat_engine import generate_response
from agents.user_input_agent import UserInputAgent
from agents.user_approval_agent import UserApprovalAgent
from agents.sample_loader_agent import SampleLoaderAgent
from agents.sql_logic_builder_agent import SQLLogicBuilderAgent
from agents.sop_validator_agent import SOPValidatorAgent
from agents.scheduler_agent import SchedulerAgent
from agents.csv_uploader_agent import CSVUploaderAgent

st.set_page_config(page_title="💬 Smart ETL Chat Assistant", layout="wide")

# Initial session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "state" not in st.session_state:
    st.session_state.state = {"current_step": "user_input"}

# Replay old messages
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# Get user input
prompt = st.chat_input("Type your response here...")

if prompt:
    # Display and store user message
    st.chat_message("user").markdown(prompt)
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Load state
    state = st.session_state.state
    step = state.get("current_step")

    try:
        # AGENT ROUTING
        if step == "user_input":
            state = UserInputAgent()(state)

        elif step == "csv_upload":
            state = CSVUploaderAgent()(state)

        elif step == "load_sample_data":
            state = SampleLoaderAgent()(state)

        elif step == "generate_sql_logic":
            state = SQLLogicBuilderAgent()(state)

        elif step == "user_sql_approval":
            state = UserApprovalAgent()(state, approval_for="sql_logic")
            if state.get("approval_status") != "approved":
                state["current_step"] = "generate_sql_logic"

        elif step == "validate_sop":
            state = SOPValidatorAgent()(state)

        elif step == "user_sop_approval":
            state = UserApprovalAgent()(state, approval_for="sop_sql")
            if state.get("approval_status") != "approved":
                state["current_step"] = "validate_sop"

        elif step == "schedule_task":
            state = SchedulerAgent()(state)

    except Exception as e:
        error_msg = f"❌ Error: {str(e)}"
        st.chat_message("ai").markdown(error_msg)
        st.session_state.messages.append({"role": "ai", "content": error_msg})

    # Save state
    st.session_state.state = state

    # Final output or fallback to LLM
    if "final_message" in state:
        st.chat_message("ai").markdown(state["final_message"])
        st.session_state.messages.append({"role": "ai", "content": state["final_message"]})
    else:
        response = generate_response(st.session_state.messages[:-1], prompt)
        st.chat_message("ai").write(response)
        st.session_state.messages.append({"role": "ai", "content": response})
