from typing import Dict
from utils.llm_provider import model
import json

class SQLLogicBuilderAgent:
    """
    Converts user-described business logic into raw SQL using Groq-hosted LLM.
    """

    def __init__(self, input_mode="cli"):
        self.input_mode = input_mode

    def __call__(self, state: Dict) -> Dict:
        print("\n🟣 [SQLLogicBuilderAgent] - Building SQL from business problem")

        if self.input_mode == "cli":
            problem = input("🧠 Describe your business logic (natural language): ").strip()

        elif self.input_mode == "streamlit":
            config = state.get("sql_logic_config", {})
            problem = config.get("problem_statement")

            if not problem:
                raise ValueError("Missing 'problem_statement' in sql_logic_config (streamlit mode)")

        else:
            raise ValueError(f"Unsupported input_mode: {self.input_mode}")

        prompt = self._build_prompt(problem, state)

        # Call Groq LLM
        response = model.invoke(prompt)
        sql_code = response.content.strip()

        return {
            **state,
            "sql_logic": {
                "problem_statement": problem,
                "generated_sql": sql_code
            },
            "current_step": "sql_logic_builder"
        }


    def _build_prompt(self, business_problem: str, state: Dict) -> str:
        # tables = state.get("data_source", {}).get("tables", [])
        database = state.get("data_source", {}).get("database", "")
        schema = state.get("data_source", {}).get("schema", "")
        # column_suggestions = state.get("llm_column_suggestions", {})

        table_descriptions = ""

        # for table in tables:
        #     try:
        #         parsed = json.loads(column_suggestions[table]) if isinstance(column_suggestions[table], str) else column_suggestions[table]
        #         columns = parsed["columns"]

        #         column_lines = "\n".join(
        #             [f'  - {col["column_name"]} ({col["data_type"]}) ← originally {col["original_column"]}' for col in columns]
        #         )

        #         table_descriptions += f"\nTable `{table}`:\n{column_lines}\n"

        #     except Exception as e:
        #         table_descriptions += f"\n⚠️ Could not parse column suggestions for `{table}`: {e}\n"

        enterprise_metadata = state.get("enterprise_metadata", {})
        table_descriptions = ""

        for table, columns in enterprise_metadata.items():
            column_lines = "\n".join(
                [f"  - {col['column_name']} ({col['data_type']})" for col in columns]
            )
            table_descriptions += f"\nTable `{table}`:\n{column_lines}\n"
            
        print(table_descriptions)


        prompt = f"""
    You are a senior Snowflake SQL developer.

    Use the following metadata to write a **clean CREATE OR REPLACE TABLE SQL query**:

    Database: {database}
    Schema: {schema}
    Tables:
    {table_descriptions}

    🧠 Business Requirement:
    {business_problem}

    🛠️ Guidelines:
    - Use ONLY the table names and columns listed above
    - Use table and column aliases for readability
    - Always qualify tables with "{database}.{schema}" (e.g. {database}.{schema}.table_name)
    - Format the SQL professionally (indentation, joins, aliases)
    - Do NOT include any explanation or comments
    - Return ONLY the final SQL query

    """.strip()

        return prompt


