from typing import Dict, Any
from utils.snowflake_utils import get_snowflake_connection
import os
from dotenv import load_dotenv
import json

load_dotenv()

AGENT_STATE_HINT = {
    "requires": ["chatbot_messages", "sop_sql", "selected_database", "selected_schema"],
    "produces": ["task_schedule", "task_sql"]
}

def SchedulerAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("🕓 SchedulerAgent invoked")

        messages = state.get("chatbot_messages", [])
        task_schedule = state.get("task_schedule")
        last_user_msg = next((m["text"].strip() for m in reversed(messages) if m["sender"] == "user"), None)

        # 🟢 If schedule not yet received, but user just replied with one
        if not task_schedule and last_user_msg and ("cron" in last_user_msg.lower() or "hour" in last_user_msg.lower()):
            print(f"✅ Schedule provided: {last_user_msg}")
            state["task_schedule"] = last_user_msg

            db = state.get("selected_database", "SAMPLE_DB")
            schema = state.get("selected_schema", "SAMPLE_SCHEMA")
            sql_code = state["sop_sql"]["refined_sql"]
            task_name = "auto_generated_task"
            warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

            task_sql = f"""
                CREATE TASK IF NOT EXISTS {task_name}
                WAREHOUSE = {warehouse}
                SCHEDULE = '{last_user_msg}'
                AS
                {sql_code}
            """.strip()

            try:
                conn = get_snowflake_connection()
                cursor = conn.cursor()
                cursor.execute(f"USE DATABASE {db}")
                cursor.execute(f"USE SCHEMA {schema}")
                cursor.execute(task_sql)
                cursor.execute(f"ALTER TASK {task_name} RESUME")
                cursor.close()
                conn.close()

                # ✅ Save in state
                state["task_sql"] = task_sql

                messages.append({
                    "sender": "assistant",
                    "text": f"🚀 Task `{task_name}` created and scheduled successfully with schedule `{last_user_msg}`!\n\n```sql\n{task_sql}\n```"
                })

                # Save the entire state to a JSON file
                with open("state_with_scheduler.json", "w") as f:
                    json.dump(state, f, indent=4, default=str)
            except Exception as e:
                messages.append({
                    "sender": "assistant",
                    "text": f"❌ Failed to schedule task: {e}"
                })

        # 🟡 Ask only once for schedule
        elif not task_schedule:
            already_prompted = any("please enter a schedule" in m["text"].lower() for m in messages if m["sender"] == "assistant")
            if not already_prompted:
                messages.append({
                    "sender": "assistant",
                    "text": "⏰ Please enter a schedule for the task (e.g., `1 hour` or `USING CRON 0 12 * * * UTC`)."
                })

        state["chatbot_messages"] = messages
        return state

    return invoke
