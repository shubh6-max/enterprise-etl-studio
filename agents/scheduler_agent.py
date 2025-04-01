from typing import Dict
from utils.snowflake_utils import get_snowflake_connection


class SchedulerAgent:
    """
    Schedules the refined SQL as a recurring Snowflake Task.
    """

    def __init__(self, input_mode="cli"):
        self.input_mode = input_mode

    def __call__(self, state: Dict) -> Dict:
        print("\n⏱️ [SchedulerAgent] - Scheduling SQL Task in Snowflake")

        sop_sql = state.get("sop_sql", {}).get("refined_sql", "")
        sop_sql = sop_sql.replace("```sql", "").replace("```", "").strip().rstrip(";")  # ✅ Remove trailing semicolon(s)

        
        

        if not sop_sql:
            raise ValueError("❌ No SOP SQL found in state")

        db = state["data_source"]["database"]
        schema = state["data_source"]["schema"]

        if self.input_mode == "cli":
            task_name = input("📛 Enter task name: ").strip()
            schedule = input("🕒 Enter schedule (e.g., '1 hour' or 'USING CRON 0 12 * * * UTC'): ").strip()
            if not schedule.startswith("'"):
                schedule = f"'{schedule}'"
            warehouse = input("🏗️ Enter Snowflake warehouse to use: ").strip()

        elif self.input_mode == "streamlit":
            cfg = state.get("schedule_config", {})
            task_name = cfg.get("task_name")
            schedule = cfg.get("schedule")
            warehouse = cfg.get("warehouse")

            if not task_name or not schedule or not warehouse:
                raise ValueError("Missing required schedule_config fields: task_name, schedule, or warehouse")

            if not schedule.startswith("'"):
                schedule = f"'{schedule}'"

        else:
            raise ValueError(f"Unsupported input_mode: {self.input_mode}")


        task_sql = f"""
CREATE OR REPLACE TASK "{db}"."{schema}"."{task_name}"
    WAREHOUSE = {warehouse}
    SCHEDULE = {schedule}
AS
{sop_sql};
        """

        resume_sql = f"""
        ALTER TASK "{db}"."{schema}"."{task_name}" RESUME;
        """

        print("\n📤 Executing CREATE TASK...")
        try:
            conn = get_snowflake_connection()
            cursor = conn.cursor()

            cursor.execute(task_sql)
            print(f"✅ Task '{task_name}' created.")

            cursor.execute(resume_sql)
            print(f"🚀 Task '{task_name}' resumed and is now active.")

            cursor.close()
            conn.close()

            return {
                **state,
                "task_status": {
                    "task_name": task_name,
                    "scheduled": True,
                    "sql": task_sql
                },
                "current_step": "schedule_task"
            }

        except Exception as e:
            return {
                **state,
                "task_status": {
                    "task_name": task_name,
                    "scheduled": False,
                    "error": str(e),
                    "sql": task_sql
                },
                "current_step": "schedule_task"
            }
