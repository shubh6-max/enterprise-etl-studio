import os
import streamlit as st
import pandas as pd
from datetime import datetime
from pprint import pprint
# Create downloadable JSON
import json
from datetime import datetime

from agents.user_input_agent import UserInputAgent
from agents.user_approval_agent import UserApprovalAgent
from agents.sample_loader_agent import SampleLoaderAgent
from agents.sql_logic_builder_agent import SQLLogicBuilderAgent
from agents.sop_validator_agent import SOPValidatorAgent
from agents.scheduler_agent import SchedulerAgent
from agents.csv_uploader_agent import CSVUploaderAgent

from utils.snowflake_utils import get_snowflake_tables
from utils.email_utils import send_email_with_json


st.set_page_config(page_title="LangGraph ETL", layout="wide")



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


# --- Init State ---
if "state" not in st.session_state:
    st.session_state.state = {"step_outputs": {}}
state = st.session_state.state

# --- Approval Runner ---
def run_user_approval(step_name: str, output: dict):
    st.subheader(f"🟢 Review & Approve Step: `{step_name}`")
    st.json(output)

    approval = st.radio("Do you approve this step?", ["approved", "rejected"], key=f"{step_name}_approval")
    submitted = st.button("Submit Approval", key=f"{step_name}_submit")

    if submitted:
        state["approval_config"] = {step_name: approval}
        agent = UserApprovalAgent(input_mode="streamlit")
        state.update(agent(state))
        return state["approval_status"]

    return None

# --- Step 0: Choose Input Type ---
source_type = st.radio("Select Data Source Type:", ["local", "snowflake"], key="source_type")

if "user_input_config" not in st.session_state:
    st.session_state.user_input_config = {}
if "run_user_input_agent" not in st.session_state:
    st.session_state.run_user_input_agent = False

# --- Step 1: Data Selection ---
if source_type == "local":
    st.subheader("📁 Step 1: Select Local CSV Tables")
    folder_path = st.text_input("Enter folder path with CSV files:", key="folder_path")

    if folder_path and os.path.exists(folder_path):
        if "csv_files" not in st.session_state:
            st.session_state.csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]

        selected_tables = st.multiselect(
            "Select CSVs to use (omit `.csv`)",
            [f.replace(".csv", "") for f in st.session_state.csv_files],
            key="selected_tables"
        )

        if st.button("✅ Proceed with Input Agent (Local)"):
            st.session_state.user_input_config = {
                "folder_path": folder_path,
                "tables": st.session_state.selected_tables,
                "available_files": st.session_state.csv_files
            }
            st.session_state.run_user_input_agent = True
            st.rerun()

elif source_type == "snowflake":
    st.subheader("❄️ Step 1: Select Snowflake Tables")
    db = st.text_input("Enter Snowflake DATABASE", key="snowflake_db")
    schema = st.text_input("Enter SCHEMA", key="snowflake_schema")

    if db and schema:
        try:
            if "sf_tables" not in st.session_state:
                st.session_state.sf_tables = get_snowflake_tables(db, schema)

            selected_sf_tables = st.multiselect("Select tables to use", st.session_state.sf_tables, key="selected_sf_tables")

            if st.button("✅ Proceed with Input Agent (Snowflake)"):
                st.session_state.user_input_config = {
                    "database": db,
                    "schema": schema,
                    "tables": st.session_state.selected_sf_tables,
                    "available_tables": st.session_state.sf_tables
                }
                st.session_state.run_user_input_agent = True
                st.rerun()
        except Exception as e:
            st.error(f"❌ Error fetching Snowflake tables: {e}")

if st.session_state.run_user_input_agent:
    agent = UserInputAgent(input_mode="streamlit", source_type=source_type)
    state.update(agent({"user_input_config": st.session_state.user_input_config}))
    state["step_outputs"]["user_input"] = state["data_source"]
    st.session_state.run_user_input_agent = False
    st.success("✅ UserInputAgent completed.")
    st.rerun()

if "data_source" in state and "approval_status_input" not in state:
    result = run_user_approval("user_input", state["data_source"])
    if result:
        state["approval_status_input"] = result
        if result != "approved":
            st.stop()

# --- Step 2: Sample Loader ---
if state.get("approval_status_input") == "approved" and "sample_data" not in state:
    result = SampleLoaderAgent()(state)
    state.update(result)
    state["step_outputs"]["sample_loader"] = state["sample_data"]

if "sample_data" in state and "approval_status_sample" not in state:
    result = run_user_approval("sample_loader", state["sample_data"])
    if result:
        state["approval_status_sample"] = result
        if result != "approved":
            st.stop()

# --- Show selected tables as DataFrames ---
if state.get("approval_status_sample") == "approved":
    st.subheader("📊 Sample Data Preview")
    for table_name, rows in state.get("sample_data", {}).items():
        st.markdown(f"**🔹 Table: `{table_name}`**")
        if isinstance(rows, list):
            df = pd.DataFrame(rows)
            st.dataframe(df)
        else:
            st.warning(f"⚠️ {rows}")

# --- Step 3: SQL Logic Builder ---
if state.get("approval_status_sample") == "approved" and "sql_logic" not in state:
    st.subheader("🧠 Step 3: Describe Business Logic for SQL Generation")
    problem = st.text_area("Enter business requirement (natural language):", key="problem_input")

    if st.button("🔄 Generate SQL from LLM") and problem:
        state["sql_logic_config"] = {"problem_statement": problem}
        agent = SQLLogicBuilderAgent(input_mode="streamlit")
        state.update(agent(state))
        state["step_outputs"]["sql_logic_builder"] = state["sql_logic"]
        st.rerun()

if "sql_logic" in state and "approval_status_sql" not in state:
    result = run_user_approval("sql_logic_builder", state["sql_logic"])
    if result:
        state["approval_status_sql"] = result
        if result != "approved":
            del state["sql_logic"]
            st.rerun()


# --- Step 4: SOP Validator ---
if state.get("approval_status_sql") == "approved" and "sop_sql" not in state:
    st.subheader("🧰 Step 4: Refine SQL with SOP Standards")
    result = SOPValidatorAgent()(state)
    state.update(result)
    state["step_outputs"]["sop_validator"] = state["sop_sql"]

if "sop_sql" in state and "approval_status_sop" not in state:
    result = run_user_approval("sop_validator", state["sop_sql"])
    if result:
        state["approval_status_sop"] = result
        if result != "approved":
            st.stop()


# --- Step 5: Upload CSVs to Snowflake ---
if state.get("approval_status_sop") == "approved" and state.get("data_source", {}).get("type") == "local" and "csv_upload" not in state:
    st.subheader("📤 Upload Local CSVs to Snowflake")
    upload_db = st.text_input("Target Snowflake Database", key="upload_db")
    upload_schema = st.text_input("Target Snowflake Schema", key="upload_schema")
    upload_approve = st.checkbox("I approve uploading all selected tables to the specified Snowflake location")

    if st.button("⬆️ Start Upload") and upload_db and upload_schema and upload_approve:
        state["csv_upload_config"] = {
            "database": upload_db,
            "schema": upload_schema,
            "approved": upload_approve
        }
        uploader = CSVUploaderAgent(input_mode="streamlit")
        progress_bar = st.progress(0, text="Preparing upload...")
        result = uploader(state, progress_bar=progress_bar)
        state.update(result)
        state["step_outputs"]["csv_uploader"] = state.get("csv_upload", {})

        # After upload, override data_source to become snowflake source
        table_map = state["csv_upload"]["table_map"]
        state["data_source"] = {
            "type": "snowflake",
            "database": upload_db,
            "schema": upload_schema,
            "tables": list(table_map.values()),
            "available_tables": list(table_map.values())
        }
        st.rerun()


# --- Step 6: Schedule Task ---
if state.get("approval_status_sop") == "approved" and "task_status" not in state:
    st.subheader("⏱️ Step 5: Schedule Refined SQL as a Task")
    task_name = st.text_input("Enter Task Name", key="task_name")
    schedule = st.text_input("Enter Schedule (e.g., 'USING CRON 0 8 * * * UTC')", key="schedule")
    warehouse = st.text_input("Enter Snowflake Warehouse", key="warehouse")

    if st.button("📅 Create & Run Task") and task_name and schedule and warehouse:
        state["schedule_config"] = {
            "task_name": task_name,
            "schedule": schedule,
            "warehouse": warehouse
        }
        agent = SchedulerAgent(input_mode="streamlit")
        result = agent(state)
        state.update(result)
        state["step_outputs"]["schedule_task"] = state["task_status"]
        st.rerun()

if "task_status" in state and "approval_status_task" not in state:
    result = run_user_approval("schedule_task", state["task_status"])
    if result:
        state["approval_status_task"] = result
        if result != "approved":
            st.stop()

# --- Final Summary ---
if state.get("approval_status_task") == "approved":
    st.success("🎉 ETL Workflow Completed Successfully!")
    
    final_state = {k: v for k, v in state.items() if k != "step_outputs"}
    st.json(final_state)

    task_name = final_state.get("task_status", {}).get("task_name", "etl")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{task_name}_{timestamp}.json"
    json_bytes = json.dumps(final_state, indent=2).encode("utf-8")

    st.download_button(
        label="📥 Download Summary JSON",
        data=json_bytes,
        file_name=filename,
        mime="application/json"
    )

    # --- Email Summary ---
    st.subheader("📧 Email Summary")

    recipient_emails = st.text_input("Recipient Emails (comma separated)", key="email_recipient")

    if st.button("Send Email (IN DEVELOPMENT)"):
        try:
            recipient_list = [e.strip() for e in recipient_emails.split(",") if e.strip()]
            send_email_with_json(
                recipients=recipient_list,
                subject="ETL Workflow Summary",
                json_content=json_bytes,
                filename=filename
            )
            st.success("✅ Email sent successfully!")
        except Exception as e:
            st.error(f"❌ Failed to send email: {e}")

