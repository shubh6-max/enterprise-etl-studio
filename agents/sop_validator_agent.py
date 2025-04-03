from typing import Dict
from utils.llm_provider import model

class SOPValidatorAgent:
    """
    Enhances raw SQL with enterprise-grade standards:
    - aliases, joins, formatting, casing, naming conventions
    """

    def __call__(self, state: Dict) -> Dict:
        print("\n🟢 [SOPValidatorAgent] - Applying SQL SOP Enhancements")

        database = state.get("data_source", {}).get("database", [])
        schema = state.get("data_source", {}).get("schema", [])
        raw_sql = state.get("sql_logic", {}).get("generated_sql", "")
        enterprise_metadata = state.get("enterprise_metadata", {})

        table_descriptions = ""

        for table, columns in enterprise_metadata.items():
            column_lines = "\n".join(
                [f"  - {col['column_name']} ({col['data_type']})" for col in columns]
            )
            table_descriptions += f"\nTable `{table}`:\n{column_lines}\n"
            
        print(table_descriptions)


        if not raw_sql:
            raise ValueError("Missing generated SQL in state")

        prompt = f"""
            You are a senior data engineer.

            Please refactor the following SQL query to match enterprise-grade standards:
            - Add table aliases
            - Use meaningful column aliases
            - Format the SQL with proper indentation
            - Use explicit JOINs if needed
            - Improve readability and maintainability
            - Fully qualify tables as {database}.{schema}.table_name
            - Use column names and table name in double quotes
            - DO NOT change business logic

            Available tables: {table_descriptions}

            SQL:
            {raw_sql}

            Only return the final, cleaned SQL. No explanation.
            """


        response = model.invoke(prompt)

        sop_sql = response.content.strip()

        # Clean out markdown formatting (from LLM output)
        sop_sql = sop_sql.replace("```sql", "").replace("```", "").strip()

        # Remove trailing semicolons (they can interfere in CREATE TASK)
        sop_sql = sop_sql.rstrip(";")

        return {
                **state,
                "sop_sql": {
                    "original_sql": raw_sql,
                    "refined_sql": sop_sql
                },
                "output": {
                    "original_sql": raw_sql,
                    "refined_sql": sop_sql
                },
                "current_step": "sop_validator"
            }

