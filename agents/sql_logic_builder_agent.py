# agents/sql_logic_builder_agent.py

from typing import Dict, Any
from utils.llm_provider import model  # uses dynamic provider (Groq, Together, etc.)
import json
import datetime

AGENT_STATE_HINT = {
    "requires": ["raw_prompt", "mentioned_tables", "raw_metadata", "sample_records"],
    "produces": ["sql_logic"]
}

class SQLLogicBuilderAgent:
    def __init__(self, input_mode="streamlit"):
        self.input_mode = input_mode

    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        print("\n🟣 [SQLLogicBuilderAgent] - Generating SQL")

        # 🧠 Extract state inputs
        problem = state.get("raw_prompt")
        database = state.get("selected_database", "")
        schema = state.get("selected_schema", "")
        tables = state.get("mentioned_tables", [])
        metadata = state.get("raw_metadata", {})
        sample_data = state.get("sample_records", {})
        output_table = state.get("output_table_name", "fact_result_table")

        if not problem or not tables or not metadata:
            raise ValueError("❌ Missing required fields for SQL generation")
        

        # 🛠️ Build the final SQL generation prompt
        prompt = self._build_prompt(
            problem, database, schema, tables, metadata, sample_data, output_table
        )

        # 🧠 LLM call
        response = model.invoke(prompt)
        sql_code = response.content.strip().replace(";","")

        # Inside SQLLogicBuilderAgent.__call__ method, just before the return
        updated_state = {
            **state,
            "sql_logic": {
                "problem_statement": problem,
                "generated_sql": sql_code
            },
            "current_step": "sql_logic_builder"
        }

        # 🔽 Save JSON here
        with open("test.json", "w") as f:
            json.dump(updated_state, f, indent=4, default=str)

        return {
            **state,
            "sql_logic": {
                "problem_statement": problem,
                "generated_sql": sql_code
            },
            "current_step": "sql_logic_builder"
        }

    def _build_prompt(self, business_problem, database, schema, tables, metadata, sample_data, output_table):
        table_details = ""
        for table in tables:
            columns = metadata.get(table, [])
            sample_rows = sample_data.get(table, [])

            column_lines = "\n".join(
                [f"  - {col['name']} ({col['type']})" for col in columns]
            )
            sample_lines = json.dumps(
                sample_rows[:3],
                indent=2,
                default=str  # ✅ simple fix: converts dates to strings
            ) if sample_rows else "(no samples)"

            table_details += f"\n\n📌 Table `{table}`:\nColumns:\n{column_lines}\nSample:\n{sample_lines}\n"

        return f"""
You are a Snowflake SQL expert.

Given the following structured inputs, generate a **robust SQL query** using professional formatting, CTEs if needed, and Snowflake best practices:

📂 Database: {database}
📁 Schema: {schema}
🧠 Business Requirement: {business_problem}
📝 Output Table: {output_table}

{table_details}

🛠️ Rules:
- Use only the provided tables/columns
- Use aliases and indentation for readability
- Always qualify tables as `{database}.{schema}.table`
- Create meaningful CTEs for modular logic
- ❗ When creating a target table, always use `CREATE OR REPLACE TABLE ... AS WITH ... SELECT ...` syntax
- Do NOT use `CREATE TABLE ... (cols) WITH ... SELECT ...` — it's not valid in Snowflake
- Do NOT use `SELECT INTO`, `INSERT INTO`, or `MERGE`
- Do NOT use `CREATE TABLE IF NOT EXISTS`
- Do NOT include explanations or markdown
- Do NOT use the SQL with a `;` — it may break parser compatibility

Return only the final SQL query using valid Snowflake syntax.
""".strip()

