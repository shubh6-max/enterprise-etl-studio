from typing import List
from pydantic import BaseModel
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import Runnable
from utils.llm_provider import get_llm

# ✅ Prompt understanding model for earlier use
class PromptUnderstanding(BaseModel):
    task: str
    tables: List[str]
    output_table: str

# LLM chain for understanding user input
prompt_template = ChatPromptTemplate.from_messages([
    ("system", "You are a senior data analyst. Clean up user requests and return structured SQL logic inputs."),
    ("user", "Given the task: {raw_prompt}\n\nMentioned tables: {tables}\n\nReturn a JSON with:\n- task\n- tables\n- output_table")
])

chain: Runnable = prompt_template | get_llm().with_structured_output(PromptUnderstanding)

def refine_user_prompt_with_llm(raw_prompt: str, tables: List[str]) -> dict:
    try:
        result: PromptUnderstanding = chain.invoke({
            "raw_prompt": raw_prompt,
            "tables": ", ".join(tables)
        })
        return result.dict()
    except Exception as e:
        return {
            "task": raw_prompt,
            "tables": tables,
            "output_table": "fact_result_table"
        }

def get_column_standardization_chain() -> Runnable:
    prompt = ChatPromptTemplate.from_template("""
You are a data engineering assistant. Given raw column metadata from a Snowflake table,
suggest standardized column names and types based on enterprise naming conventions.

Input:
- Table name: {table_name}
- Columns: {columns}

Output JSON format:
{{ 
  "columns": [
    {{
      "original": "order_id",
      "suggested": "order_id",
      "type": "NUMBER",
      "comment": "Unique identifier for order"
    }},
    ...
  ]
}}
Only return the JSON object — no explanation.
""")

    model = get_llm()

    return prompt | model.with_structured_output(schema={
        "title": "ColumnSuggestions",
        "description": "Suggested column names, types, and comments",
        "type": "object",
        "properties": {
            "columns": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "original": {"type": "string"},
                        "suggested": {"type": "string"},
                        "type": {"type": "string"},
                        "comment": {"type": "string"}
                    },
                    "required": ["original", "suggested", "type"]
                }
            }
        },
        "required": ["columns"]
    })

