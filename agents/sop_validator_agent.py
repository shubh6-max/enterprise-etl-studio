# agents/sop_validator_agent.py

from typing import Dict, Any
from utils.llm_provider import model  # Groq/Anthropic/etc.
import json


AGENT_STATE_HINT = {
    "requires": ["sql_logic", "raw_metadata", "selected_database", "selected_schema"],
    "produces": ["sop_sql", "output"]
}

class SOPValidatorAgent:
    """
    Enhances raw SQL with enterprise-grade formatting, casing, joins, aliasing, etc.
    """

    def __call__(self, state: Dict[str, Any]) -> Dict[str, Any]:
        print("🟢 [SOPValidatorAgent] - Enhancing SQL")


        database = state.get("selected_database", "")
        schema = state.get("selected_schema", "")
        raw_sql = state.get("sql_logic", {}).get("generated_sql", "")
        metadata = state.get("raw_metadata", {})

        if not raw_sql:
            raise ValueError("❌ No generated SQL found in state")
    
        # 🔍 Prepare metadata summary
        table_descriptions = ""
        for table, columns in metadata.items():
            col_lines = "\n".join([f"  - {col['name']} ({col['type']})" for col in columns])
            table_descriptions += f"\n📁 Table `{table}`:\n{col_lines}\n"

        # 🧠 Build SOP enforcement prompt
        prompt = f"""
You are a senior Snowflake SQL developer.

Refactor the following SQL query to comply with enterprise-grade best practices and ensure 100% compatibility with Snowflake:

✅ Rules:
- Add table aliases
- Use readable column aliases
- Apply clean indentation and formatting
- Fully qualify all tables as: {database}.{schema}.table
- Keep business logic exactly the same — no changes in logic or filters
- Remove double quotes from column names or aliases
- Correct any syntax errors or inconsistencies
- Ensure the final query is executable in Snowflake
- ❗ When creating a target table, always use: `CREATE OR REPLACE TABLE ... AS WITH ... SELECT ...`
- 🚫 Do NOT use `CREATE TABLE ... (...) WITH ... SELECT ...`
- 🚫 Do NOT use `SELECT INTO`, `INSERT`, `UPDATE`, `DELETE`, or `MERGE`
- 🚫 Do NOT use `CREATE TABLE IF NOT EXISTS`
- 🚫 Do NOT include semicolons (`;`)
- Output only the cleaned and executable SQL — no explanations, markdown, or comments

Available tables:
{table_descriptions}

SQL to refine:
{raw_sql}
""".strip()


        response = model.invoke(prompt)
        refined_sql = response.content.strip()
        refined_sql = refined_sql.replace("```sql", "").replace("```", "").rstrip(";")

        state["chatbot_messages"].append({
            "sender": "assistant",
            "text": f"🛠️ Refined SQL (SOP-compliant):\n```sql\n{refined_sql}\n```"
            })

        final_state = {
            **state,
            "sop_sql": {
                "original_sql": raw_sql,
                "refined_sql": refined_sql
            },
            "output": {
                "original_sql": raw_sql,
                "refined_sql": refined_sql
            },
            "current_step": "sop_validator"
        }

        # Save to a JSON file
        with open("state_with_sop.json", "w") as f:
            json.dump(final_state, f, indent=4, default=str)

        return {
            **state,
            "sop_sql": {
                "original_sql": raw_sql,
                "refined_sql": refined_sql
            },
            "output": {
                "original_sql": raw_sql,
                "refined_sql": refined_sql
            },
            "current_step": "sop_validator"
        }
