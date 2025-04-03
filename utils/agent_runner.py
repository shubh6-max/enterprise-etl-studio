import copy
import streamlit as st
from agents.user_approval_agent import UserApprovalAgent
import pandas as pd

def run_agent_step_simple(state, step_key, agent_class, agent_input_key=None, approval_required=True):
    output_key = f"{step_key}_output"
    approval_key = f"approval_status_{step_key}"

    # Run agent if output not already present
    if output_key not in state:
        agent = agent_class()
        input_data = state.get(agent_input_key) if agent_input_key else state
        result = agent(input_data)
        state.update(result)
        state["step_outputs"][step_key] = result.get("output", result)
        state[output_key] = state["step_outputs"][step_key]

    # Approval block
    if approval_required and approval_key not in state:
        st.subheader(f"🔎 Review & Approve: `{step_key}`")
        st.json(state[output_key])
        approval = st.radio("Do you approve this step?", ["approved", "rejected"], key=f"{step_key}_approval")
        if st.button("✅ Submit Approval", key=f"{step_key}_submit"):
            state["approval_config"] = {step_key: approval}
            agent = UserApprovalAgent(input_mode="streamlit")
            state.update(agent(state))
            state[approval_key] = approval
            if approval != "approved":
                st.warning(f"⛔ Step `{step_key}` was not approved. Halting.")
                st.stop()

def run_agent_step_with_ui(state, agent_class, step_name: str, approval_required=True, button_label=None, agent_kwargs=None):

    step_key = step_name.lower()
    output_key = f"{step_key}_output"
    approval_key = f"approval_status_{step_key}"

    st.subheader(f"📌 Step: {step_name.replace('_', ' ').title()}")

    if output_key not in state:
        if not button_label:
            button_label = f"🚀 Run {step_name.replace('_', ' ').title()}"
        if st.button(button_label, key=f"{step_key}_run"):
            agent = agent_class(**(agent_kwargs or {}))
            result = agent(state)
            state.update(result)
            state["step_outputs"][step_key] = result.get("output", result)
            state[output_key] = result.get("output", result)
            st.rerun()

    # ✅ Handle display logic based on agent type
    if output_key in state:
        st.markdown(f"✅ Output from `{step_name}`:")

        if step_key == "sample_loader":
            sample_data = state[output_key].get("sample_data", {})
            for table, rows in sample_data.items():
                st.markdown(f"**🔹 Table: `{table}`**")
                if isinstance(rows, list):
                    st.dataframe(pd.DataFrame(rows))
                else:
                    st.warning(rows)
        else:
            # 🔁 Fallback to JSON with circular-safe view
            try:
                safe_output = copy.deepcopy(state[output_key])
                if isinstance(safe_output, dict):
                    safe_output.pop("step_outputs", None)
                    safe_output.pop("data_source", None)
                st.json(safe_output)
            except Exception as e:
                st.warning(f"⚠️ JSON serialization failed: {e}")
                st.write(state[output_key])

        if approval_required and approval_key not in state:
            decision = run_user_approval(step_key, state[output_key])
            if decision:
                state[approval_key] = decision
                if decision != "approved":
                    st.warning(f"⛔ Step `{step_name}` was not approved. Stopping workflow.")
                    st.stop()



def run_user_approval(step_name: str, output: dict):
    st.subheader(f"🔍 Review & Approve Step: {step_name}")

    # Avoid circular reference issues
    try:
        # Shallow copy and exclude large/state-like entries
        safe_output = copy.deepcopy(output)
        if isinstance(safe_output, dict):
            safe_output.pop("step_outputs", None)
            safe_output.pop("user_input_config", None)
            safe_output.pop("data_source", None)
            safe_output.pop("sample_data", None)
        st.json(safe_output)
    except Exception as e:
        st.warning(f"⚠️ Could not render output as JSON: {e}")
        st.write(output)

    approval = st.radio("Do you approve this step?", ["approved", "rejected"], key=f"{step_name}_approval")
    if st.button("✅ Submit Approval", key=f"{step_name}_submit"):
        st.session_state.state["approval_config"] = {step_name: approval}
        from agents.user_approval_agent import UserApprovalAgent
        agent = UserApprovalAgent(input_mode="streamlit")
        st.session_state.state.update(agent(st.session_state.state))
        return st.session_state.state["approval_status"]

    return None
