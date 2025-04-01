from typing import Dict
from utils.llm_provider import model

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
        tables = state.get("data_source", {}).get("tables", [])
        return f"""
    You are an expert SQL developer.

    Based on the business requirement and available tables, write a clean, syntactically correct SQL query.

    Tables: {", ".join(tables)}
    Business requirement: {business_problem}

    Important:
    - Do NOT explain anything
    - Return ONLY the SQL query, no commentary
    """
