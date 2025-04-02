from typing import Dict
import pandas as pd
from utils.snowflake_utils import get_snowflake_connection

class SQLExecutorAgent:
    """
    Executes the SQL query and returns results as DataFrame.
    """

    def __call__(self, state: Dict) -> Dict:
        sql = state.get("sop_sql", {}).get("refined_sql", "").strip()
        if not sql:
            raise ValueError("❌ No generated SQL found to execute")

        try:
            conn = get_snowflake_connection()
            df = pd.read_sql(sql, conn)
            conn.close()

            return {
                **state,
                "sql_result": {
                    "row_count": len(df),
                    "columns": list(df.columns),
                    "preview": df.head(100).to_dict(orient="records")
                },
                "current_step": "sql_executor"
            }

        except Exception as e:
            return {
                **state,
                "sql_result": {
                    "error": str(e),
                    "preview": []
                },
                "current_step": "sql_executor"
            }
