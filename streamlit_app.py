import os
import json
import pandas as pd
from datetime import datetime
import streamlit as st

# --- Import Agents ---
from agents.user_input_agent import UserInputAgent
from agents.sample_loader_agent import SampleLoaderAgent
from agents.metadata_fetcher_agent import MetadataFetcherAgent
from agents.sql_logic_builder_agent import SQLLogicBuilderAgent
from agents.sop_validator_agent import SOPValidatorAgent
from agents.sql_executor_agent import SQLExecutorAgent
from agents.scheduler_agent import SchedulerAgent
from agents.user_approval_agent import UserApprovalAgent

from utils.agent_runner import run_agent_step_with_ui

# --- Config Streamlit ---
st.set_page_config(page_title="Enterprise ETL Studio", layout="wide")

st.markdown("""
    <style>
    .stApp {
        background-color: #f4f6fa;
    }

    .stHeader {
        display: flex;
        justify-content: space-between;
    }

    .logo {
        position: fixed;
        top: 70px;
        left: 20px;
        z-index: 1000;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown(
    '<img src="https://upload.wikimedia.org/wikipedia/commons/8/88/MathCo_Logo.png" class="logo" width="300" height="auto">',
    unsafe_allow_html=True
)


st.markdown(
    "<h1 style='text-align: center; color: #333;'>🏢 Enterprise ETL Studio</h1>",
    unsafe_allow_html=True
)

# --- Initialize Session State ---
if "state" not in st.session_state:
    st.session_state.state = {"step_outputs": {}}
state = st.session_state.state


# --- Reusable Approval Block ---
def run_user_approval(step_name: str, output: dict):
    st.subheader(f"🔎 Review & Approve: `{step_name}`")
    st.json(output)
    decision = st.radio("Do you approve this step?", ["approved", "rejected"], key=f"{step_name}_approval")
    if st.button("✅ Submit Approval", key=f"{step_name}_submit"):
        state["approval_config"] = {step_name: decision}
        agent = UserApprovalAgent(input_mode="streamlit")
        state.update(agent(state))
        return state["approval_status"]
    return None

# --------------------------- STEP 1: USER INPUT AGENT ---------------------------
source_type = st.radio("Select Data Source Type:", ["local", "snowflake"], key="source_type")

if "user_input_config" not in st.session_state:
    st.session_state.user_input_config = {}
if "run_user_input_agent" not in st.session_state:
    st.session_state.run_user_input_agent = False

if source_type == "local":
    st.subheader("📁 Step 1: Select Local CSV Tables")
    folder_path = st.text_input("Enter folder path with CSV files:", key="folder_path").replace('"','')

    upload_db = st.text_input("Enter Snowflake Database", key="upload_db")
    upload_schema = st.text_input("Enter Snowflake Schema", key="upload_schema")

    if folder_path and os.path.exists(folder_path):
        if "csv_files" not in st.session_state:
            st.session_state.csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

        selected_tables = st.multiselect(
            "Select CSVs to use (omit `.csv`) 👉",
            [f.replace(".csv", "") for f in st.session_state.csv_files],
            key="selected_tables"
        )

        if st.button("✅ Proceed with Input Agent (Local)") and upload_db and upload_schema:
            st.session_state.user_input_config = {
                "folder_path": folder_path,
                "tables": st.session_state.selected_tables,
                "available_files": st.session_state.csv_files,
                "database": upload_db,
                "schema": upload_schema
            }
            st.session_state.run_user_input_agent = True
            st.rerun()


elif source_type == "snowflake":
    from utils.snowflake_utils import get_snowflake_tables
    db = st.text_input("Enter Snowflake DATABASE", key="sf_db")
    schema = st.text_input("Enter SCHEMA", key="sf_schema")
    if db and schema:
        try:
            st.session_state.sf_tables = get_snowflake_tables(db, schema)
            selected = st.multiselect("Select Snowflake tables", st.session_state.sf_tables, key="selected_sf")
            if st.button("Proceed with Snowflake"):
                st.session_state.user_input_config = {
                    "database": db,
                    "schema": schema,
                    "tables": selected,
                    "available_tables": st.session_state.sf_tables
                }
                st.session_state.run_user_input_agent = True
                st.rerun()
        except Exception as e:
            st.error(f"❌ {e}")

if st.session_state.run_user_input_agent:
    agent = UserInputAgent(input_mode="streamlit", source_type=source_type)
    state.update(agent({"user_input_config": st.session_state.user_input_config}))
    state["step_outputs"]["user_input"] = state["data_source"]
    st.session_state.run_user_input_agent = False
    st.rerun()

# --- Approval for User Input ---
if "data_source" in state and "approval_status_input" not in state:
    result = run_user_approval("user_input", state["data_source"])
    if result:
        state["approval_status_input"] = result
        if result != "approved":
            st.stop()
# --------------------------- STEP 2: SAMPLE LOADER ---------------------------
if state.get("approval_status_input") == "approved" and "approval_status_sample_loader" not in state:
    agent = SampleLoaderAgent()
    result = agent(state)
    state.update(result)
    state["step_outputs"]["sample_loader"] = result.get("output", result)

    # Get approval
    decision = run_user_approval("sample_loader", state["step_outputs"]["sample_loader"])
    if decision:
        state["approval_status_sample_loader"] = decision
        if decision != "approved":
            st.stop()

# --------------------------- STEP 3: METADATA FETCHER ---------------------------
if (
    state.get("approval_status_sample_loader") == "approved"
    and state["data_source"]["type"] == "local"
):

    # ✅ Only run the agent if metadata_fetcher hasn't run yet
    if "metadata_fetcher" not in state.get("step_outputs", {}):
        agent = MetadataFetcherAgent()
        result = agent(state)
        state.update(result)

        state["step_outputs"]["metadata_fetcher"] = {
            "upload_report": result.get("llm_upload_report"),
            "create_sqls": result.get("llm_create_sql"),
            "sample_data": result.get("sample_data"),
            "enterprise_metadata": result.get("enterprise_metadata"),
        }

        # Optional: Save snapshot
        with open("final_state_snapshots/state_metadata_fetcher.json", "w") as f:
            json.dump(state, f, indent=4)

    # ✅ Now handle approval separately
    if "approval_status_metadata_fetcher" not in state:
        decision = run_user_approval("metadata_fetcher", state["step_outputs"]["metadata_fetcher"])
        if decision:
            state["approval_status_metadata_fetcher"] = decision
            if decision != "approved":
                st.stop()

# --------------------------- STEP 4: SQL LOGIC BUILDER ---------------------------
# --- Step 3: SQL Logic Builder ---
# --------------------------- STEP 4: SQL LOGIC BUILDER ---------------------------
if (
    state.get("approval_status_sample_loader") == "approved"
    and (
        state["data_source"]["type"] == "snowflake"
        or state.get("approval_status_metadata_fetcher") == "approved"
    )
    and "approval_status_sql_logic_builder" not in state
):
    st.subheader("🧠 Step 4: Describe Business Logic for SQL Generation")

    problem = st.text_area("Enter business requirement (natural language):", key="problem_input")

    if st.button("🔄 Generate SQL from LLM") and problem:
        state["sql_logic_config"] = {"problem_statement": problem}
        agent = SQLLogicBuilderAgent(input_mode="streamlit")
        result = agent(state)
        state.update(result)
        state["step_outputs"]["sql_logic_builder"] = result["sql_logic"]
        st.rerun()

if "sql_logic" in state and "approval_status_sql_logic_builder" not in state:
    decision = run_user_approval("sql_logic_builder", state["sql_logic"])
    if decision:
        state["approval_status_sql_logic_builder"] = decision
        if decision != "approved":
            del state["sql_logic"]
            st.rerun()

# --------------------------- STEP 5: SOP VALIDATOR ---------------------------
if state.get("approval_status_sql_logic_builder") == "approved" and "approval_status_sop_validator" not in state:
    st.subheader("✅ Step 5: SOP Validator")

    # Run the SOPValidatorAgent
    agent = SOPValidatorAgent()
    result = agent(state)
    state.update(result)

    # Save result in step_outputs
    state["step_outputs"]["sop_validator"] = result.get("sop_sql")

    # Approval
    decision = run_user_approval("sop_validator", state["step_outputs"]["sop_validator"])
    if decision:
        state["approval_status_sop_validator"] = decision
        if decision != "approved":
            st.warning("❌ SOP validation rejected.")
            st.stop()

    # Snapshot
    with open("final_state_snapshots/state_sop_validator.json", "w") as f:
        json.dump(state, f, indent=4)


# --------------------------- STEP 6: SQL EXECUTOR (Preview) ---------------------------
if state.get("approval_status_sop_validator") == "approved" and "sql_executor_output" not in state:
    agent = SQLExecutorAgent()
    result = agent(state)
    state.update(result)

    state["step_outputs"]["sql_executor"] = result.get("sql_result")
    state["sql_executor_output"] = result.get("sql_result")

    # Save snapshot
    with open("final_state_snapshots/state_sql_executor.json", "w") as f:
        json.dump(state, f, indent=4)

# Display output
if "sql_executor_output" in state:
    result = state["sql_executor_output"]
    if result.get("error"):
        st.error(f"❌ SQL Error: {result['error']}")
    else:
        st.success(f"✅ Preview: {result['row_count']} rows")
        st.dataframe(pd.DataFrame(result["preview"]))

# --------------------------- STEP 7: SCHEDULER ---------------------------
if state.get("approval_status_sop_validator") == "approved":
    st.subheader("⏱️ Step 7: Schedule SQL as Task")

    if "schedule_config" not in state:
        task_name = st.text_input("Task Name", key="task_name")
        schedule = st.text_input("Schedule (e.g., '1 hour' or 'USING CRON 0 12 * * * UTC')", key="schedule")
        warehouse = st.text_input("Snowflake Warehouse", key="warehouse")

        if st.button("📅 Schedule Task"):
            state["schedule_config"] = {
                "task_name": task_name,
                "schedule": schedule,
                "warehouse": warehouse
            }
            st.rerun()

# Run the agent
if "schedule_config" in state and "task_status" not in state:
    agent = SchedulerAgent(input_mode="streamlit")
    result = agent(state)
    state.update(result)

    state["step_outputs"]["schedule_task"] = result.get("task_status")

    # Save snapshot
    with open("final_state_snapshots/state_schedule_task.json", "w") as f:
        json.dump(state, f, indent=4)

# Approval
if "task_status" in state and "approval_status_schedule_task" not in state:
    result = run_user_approval("schedule_task", state["task_status"])
    if result:
        state["approval_status_schedule_task"] = result
        if result != "approved":
            st.stop()

# --------------------------- STEP 8: FINAL SUMMARY ---------------------------
if state.get("approval_status_schedule_task") == "approved":
    st.success("🎉 ETL Workflow Completed Successfully!")

    # Optional: Save snapshot
    with open("final_state_snapshots/state_metadata_fetcher.json", "w") as f:
        json.dump(state, f, indent=4)

    def sanitize_for_json(data):
        try:
            json.dumps(data)
            return data
        except:
            return str(data)

    # Only non-agent metadata
    final_state = {k: v for k, v in state.items() if k != "step_outputs"}
    json_ready = {k: sanitize_for_json(v) for k, v in final_state.items()}

    st.subheader("📦 Final State Summary")
    st.json(json_ready)

    # ⬇️ Download Summary
    task = state.get("task_status", {}).get("task_name", "etl_workflow")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{task}_{timestamp}.json"
    json_bytes = json.dumps(json_ready, indent=2).encode("utf-8")

    st.download_button(
        "📥 Download Summary JSON",
        data=json_bytes,
        file_name=filename,
        mime="application/json"
    )

    # 📧 Optional Email
    st.subheader("📧 Send Summary via Email")
    recipient_emails = st.text_input("Recipient Email(s) (comma-separated)")

    if st.button("📨 Send Email Summary"):
        try:
            from utils.email_utils import send_email_with_json

            recipients = [email.strip() for email in recipient_emails.split(",") if email.strip()]
            send_email_with_json(
                recipients=recipients,
                subject=f"ETL Summary: {task}",
                json_content=json_bytes,
                filename=filename
            )
            st.success("✅ Email sent successfully!")
        except Exception as e:
            st.error(f"❌ Failed to send email: {e}")

