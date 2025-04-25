# agents/cte_extractor_agent.py

from typing import Dict, Any, List
from utils.llm_provider import model
import json
import re

AGENT_STATE_HINT = {
    "requires": ["sql_logic"],
    "produces": ["staging_tables", "final_table"]
}

class CTEExtractorAgent:
    def __init__(self, input_mode="streamlit"):
        self.input_mode = input_mode

    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        print("\n🔵 [CTEExtractorAgent] - Extracting CTEs and Creating Staging Tables")

        # 🧠 Extract state inputs
        sql_logic = state.get("output", {})
        sql_query = sql_logic.get("refined_sql", "")
        database = state.get("selected_database", "")
        schema = state.get("selected_schema", "")
        final_table_name = state.get("final_table_name", "final_output")
        
        if not sql_query:
            # Look for an alternative field in refined_sql if available
            if "sop_sql" in state and "refined_sql" in state["sop_sql"]:
                sql_query = state["sop_sql"]["refined_sql"]
            elif "output" in state and "refined_sql" in state["output"]:
                sql_query = state["output"]["refined_sql"]
            
            # If still no SQL query, raise error
            if not sql_query:
                raise ValueError("❌ Missing required SQL query for CTE extraction")

        # 🛠️ Build the CTE extraction prompt
        prompt = self._build_prompt(sql_query, database, schema, final_table_name)

        # 🧠 LLM call
        response = model.invoke(prompt)
        cte_extraction_result = response.content.strip()
        
        # Parse the JSON response
        try:
            tables_data = json.loads(cte_extraction_result)
            # Separate staging tables and final table
            staging_tables = [t for t in tables_data if t.get("is_final_table", False) is False]
            final_table = next((t for t in tables_data if t.get("is_final_table", True)), None)
        except json.JSONDecodeError:
            # Fallback parsing if not proper JSON
            tables_data = self._extract_json_from_text(cte_extraction_result)
            staging_tables = [t for t in tables_data if t.get("is_final_table", False) is False]
            final_table = next((t for t in tables_data if t.get("is_final_table", True)), None)
            
        # Update state with the staging tables and final table information
        updated_state = {
            **state,
            "staging_tables": staging_tables,
            "final_table": final_table,
            "current_step": "cte_extractor"
        }

        # 🔽 Save JSON here - include workflow status if available
        if "workflow_status" in state:
            updated_state["workflow_status"] = state["workflow_status"]
            
        with open("cte_extraction_state.json", "w") as f:
            json.dump(updated_state, f, indent=4, default=str)

        return updated_state

    def _extract_json_from_text(self, text: str) -> List[Dict]:
        """Extract JSON objects from text if LLM doesn't return clean JSON."""
        # Find JSON pattern between ```json and ```
        json_match = re.search(r'```json\s*([\s\S]*?)\s*```', text)
        if json_match:
            try:
                return json.loads(json_match.group(1))
            except:
                pass
                
        # Last resort: try to find anything that looks like JSON array
        json_match = re.search(r'\[\s*{[\s\S]*}\s*\]', text)
        if json_match:
            try:
                return json.loads(json_match.group(0))
            except:
                pass
                
        # If all else fails, return empty list
        return []

    def _build_prompt(self, sql_query, database, schema, final_table_name="final_output"):
        # Remove SQL code block markers if present
        sql_query = re.sub(r'```sql|```', '', sql_query).strip()
        
        return f"""
You are a Snowflake SQL expert specialized in ETL optimization.

Analyze the following SQL query and break it down into separate staging tables with a final output table:

```sql
{sql_query}
```

📂 Database: {database}
📁 Schema: {schema}
🎯 Final table name: {final_table_name}

🔍 Task:
1. Identify all Common Table Expressions (CTEs) in the query
2. For each intermediate CTE, create a separate CREATE OR REPLACE TRANSIENT TABLE statement
3. For the final result (the last CTE or main query), create a regular permanent CREATE OR REPLACE TABLE statement
4. Track the dependencies between all tables

🛠️ Rules:
- Every intermediate CTE becomes a transient staging table (e.g., CTE 'users' becomes 'stg_users')
- All staging tables should use CREATE OR REPLACE TRANSIENT TABLE
- The final output table should use CREATE OR REPLACE TABLE (not transient) with name {final_table_name}
- Every table should be fully qualified: {database}.{schema}.table_name
- Make sure staged tables can be built independently given their dependencies
- Make sure to preserve the order and dependencies of operations

Return the results as a JSON array of tables with this structure:
```json
[
  {{
    "table_name": "stg_table_name",
    "original_cte_name": "original_cte_name",
    "create_statement": "CREATE OR REPLACE TRANSIENT TABLE {database}.{schema}.stg_table_name AS SELECT...",
    "depends_on": ["stg_another_table"], // Names of tables this one depends on
    "is_final_table": false
  }},
  {{
    "table_name": "{final_table_name}",
    "original_cte_name": "final_query_or_cte_name",
    "create_statement": "CREATE OR REPLACE TABLE {database}.{schema}.{final_table_name} AS SELECT...",
    "depends_on": ["stg_table_name"],
    "is_final_table": true
  }}
]
```

Return only the JSON array of tables, no additional explanation.
""".strip()