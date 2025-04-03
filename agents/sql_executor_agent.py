from typing import Dict
import pandas as pd
from utils.snowflake_utils import get_snowflake_connection

class SQLExecutorAgent:
    """
    Executes the SQL query and returns results as DataFrame.
    """

    def __call__(self, state: Dict) -> Dict:
        sql = state.get("sop_sql", {}).get("refined_sql", "")
        sql = sql.replace("```sql", "").replace("```", "").strip().rstrip(";")

        if not sql:
            raise ValueError("❌ No generated SQL found to execute")

        try:
            conn = get_snowflake_connection()

            # Set context
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {state['data_source']['database']}")
            cursor.execute(f"USE SCHEMA {state['data_source']['schema']}")
            cursor.close()

            # Run SQL
            df = pd.read_sql(sql, conn)
            conn.close()

            return {
                **state,
                "sql_result": {
                    "row_count": len(df),
                    "columns": list(df.columns),
                    "preview": df.head(100).to_dict(orient="records")
                },
                "current_step": "sql_executor",
                "output": {
                    "row_count": len(df),
                    "columns": list(df.columns),
                    "preview": df.head(10).to_dict(orient="records")
                },
                "step_outputs": {
                    **state.get("step_outputs", {}),
                    "sql_executor": {
                        "row_count": len(df),
                        "columns": list(df.columns),
                        "preview": df.head(10).to_dict(orient="records")
                    }
                }
            }


        except Exception as e:
            return {
                **state,
                "sql_result": {
                    "error": str(e),
                    "preview": []
                },
                "current_step": "sql_executor",
                "output": {
                    "error": str(e),
                    "preview": []
                },
                "step_outputs": {
                    **state.get("step_outputs", {}),
                    "sql_executor": {
                        "error": str(e),
                        "preview": []
                    }
                }
            }

