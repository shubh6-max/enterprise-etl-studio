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
            - Use database,schema for refactor e.g.(database.schema.table)
            Important:
                - Use column name in  double quotes

            Return only the improved SQL, nothing else.

            database: {database}
            schema: {schema}
            SQL:
            {raw_sql}
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
            "current_step": "sop_validator"
        }
