import json
import os
from datetime import datetime
from pprint import pprint

from agents.user_input_agent import UserInputAgent
from agents.user_approval_agent import UserApprovalAgent
from agents.sample_loader_agent import SampleLoaderAgent
from agents.sql_logic_builder_agent import SQLLogicBuilderAgent
from agents.sop_validator_agent import SOPValidatorAgent
from agents.scheduler_agent import SchedulerAgent
from agents.csv_uploader_agent import CSVUploaderAgent


def save_final_state(state: dict, save_path: str = "final_state_snapshots"):
    clean_state = {k: v for k, v in state.items() if k != "step_outputs"}
    os.makedirs(save_path, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"final_state_{timestamp}.json"
    full_path = os.path.join(save_path, filename)
    with open(full_path, "w") as f:
        json.dump(clean_state, f, indent=4)
    print(f"\n📁 Final state saved to: {full_path}")

def test_pipeline(source_type: str):
    state = {}
    approval_agent = UserApprovalAgent()

    # STEP 1: User Input
    state = UserInputAgent(source_type=source_type)(state)
    state["step_outputs"] = {"user_input": state["data_source"]}
    state = approval_agent(state)
    if state["approval_status"] != "approved":
        print("❌ User rejected input. Exiting.")
        return

    # STEP 2: Sample Loader
    state = SampleLoaderAgent()(state)
    state["step_outputs"]["sample_loader"] = state["sample_data"]
    state = approval_agent(state)
    if state["approval_status"] != "approved":
        print("❌ User rejected sample data. Exiting.")
        return

    # STEP 3: SQL Logic Builder with retry loop
    attempts = 0
    while True:
        attempts += 1
        print(f"\n🔁 Attempt #{attempts} to generate SQL logic...")
        state = SQLLogicBuilderAgent()(state)
        state["step_outputs"]["sql_logic_builder"] = state["sql_logic"]
        state = approval_agent(state)
        if state["approval_status"] == "approved":
            break
        print("⚠️ User rejected SQL logic. Regenerating...")

    # STEP 4: SOP Validator
    state = SOPValidatorAgent()(state)
    state["step_outputs"]["sop_validator"] = state["sop_sql"]
    state = approval_agent(state)
    if state["approval_status"] != "approved":
        print("❌ User rejected SOP-enhanced SQL. Exiting.")
        return

    # STEP 5: If source is local, upload CSVs to Snowflake
    if state["data_source"]["type"] == "local":
        state = CSVUploaderAgent()(state)

        # Update data_source to mimic Snowflake for scheduler
        upload_info = state.get("csv_upload", {})
        state["data_source"]["type"] = "snowflake"
        state["data_source"]["database"] = upload_info.get("database")
        state["data_source"]["schema"] = upload_info.get("schema")
        state["data_source"]["tables"] = list(upload_info.get("table_map", {}).values())

    # ✅ CLEAN MARKDOWN BLOCKS from SOP SQL
    sop_sql = state.get("sop_sql", {}).get("refined_sql", "")
    state["sop_sql"]["refined_sql"] = sop_sql.replace("```sql", "").replace("```", "").strip()

    # STEP 6: Schedule SQL as Snowflake Task
    state = SchedulerAgent()(state)
    state["step_outputs"]["schedule_task"] = state.get("task_status", {})
    state = approval_agent(state)
    if state["approval_status"] != "approved":
        print("❌ User rejected Snowflake scheduling. Exiting.")
        return

    # ✅ Final Output
    print("\n✅ FINAL STATE SNAPSHOT")
    for key, value in state.items():
        if key != "step_outputs":
            print(f"{key}: {value}")

    pprint({k: v for k, v in state.items() if k != "step_outputs"}, indent=2, width=120)

    # 💾 Save to JSON
    save_final_state(state)

if __name__ == "__main__":
    print("\n🌐 Select Data Source:")
    print("1. Local Folder")
    print("2. Snowflake")

    choice = input("Enter 1 or 2: ").strip()

    if choice == "1":
        test_pipeline(source_type="local")
    elif choice == "2":
        test_pipeline(source_type="snowflake")
    else:
        print("❌ Invalid choice. Please select 1 or 2.")


# C:\Users\ShubhamVishwasPurani\OneDrive - TheMathCompany Private Limited\Desktop\LLM_Initiative\ai-etl-pipeline\Fictional Sales Data