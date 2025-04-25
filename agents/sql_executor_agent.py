# agents/sql_executor_agent.py

from typing import Dict, Any
import pandas as pd
from utils.snowflake_utils import get_snowflake_connection

AGENT_STATE_HINT = {
    "requires": ["sop_sql", "selected_database", "selected_schema"],
    "produces": ["sql_result"]
}

def SQLExecutorAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("\n🟣 [SQLExecutorAgent] - Executing final SQL...")

        sql = state.get("sop_sql", {}).get("refined_sql", "")
        if not sql:
            raise ValueError("❌ No SQL found in `sop_sql.refined_sql`")

        # Clean SQL block markers if any
        sql = sql.replace("```sql", "").replace("```", "").strip().replace(";","")

        db = state.get("selected_database")
        schema = state.get("selected_schema")

        try:
            conn = get_snowflake_connection()

            # Context setup
            cursor = conn.cursor()
            if db:
                cursor.execute(f"USE DATABASE {db}")
            if schema:
                cursor.execute(f"USE SCHEMA {schema}")
            cursor.close()

            # Execute SQL and fetch
            df = pd.read_sql(sql, conn)
            conn.close()

            result_preview = df.head(10).to_dict(orient="records")

            preview_text = pd.DataFrame(df.head(5)).to_markdown(index=False)

            state["chatbot_messages"].append({
                "sender": "assistant",
                "text": f"📊 Executed SQL! Here's a preview of the result:\n\n{preview_text}"
            })

            state["sql_execution_done"] = True
            
            print(f"✅ SQL executed successfully. Rows: {len(df)}")

            state["sql_result"] = {
                "row_count": len(df),
                "columns": list(df.columns),
                "preview": result_preview
            }

        except Exception as e:
            print(f"❌ SQL execution error: {e}")
            state["sql_result"] = {
                "error": str(e),
                "preview": []
            }
            state["chatbot_messages"].append({
                "sender": "assistant",
                "text": f"❌ Error while executing SQL: `{str(e)}`"
                })

        return state

    return invoke
