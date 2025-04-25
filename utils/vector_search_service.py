# utils/vector_search_service.py

from typing import Dict
from utils.snowflake_utils import list_all_tables

def search_similar_prompts(user_prompt: str) -> Dict:
    """
    Simulated vector search using naive keyword matching.
    Returns: {"tables": [table_name, ...]}
    """
    prompt_keywords = set(user_prompt.lower().split())
    all_tables = list_all_tables()

    matched = []

    for entry in all_tables:
        table_name = entry["name"].lower()
        if any(word in table_name for word in prompt_keywords):
            matched.append(entry["name"])

    return {"tables": list(set(matched))}
