# utils/knowledge_graph_service.py

from typing import List, Tuple
from utils.snowflake_utils import list_all_tables

def resolve_entities_from_kg(user_prompt: str) -> Tuple[List[str], List[str]]:
    """
    Naive KG-style table matcher using substring + fuzzy similarity.
    Returns: (table_matches, debug_logs)
    """
    prompt_lower = user_prompt.lower()
    all_tables = list_all_tables()

    table_matches = []
    debug_logs = []

    for entry in all_tables:
        table_name = entry["name"]
        fqdn = f"{entry['database']}.{entry['schema']}.{entry['name']}"
        
        if table_name.lower() in prompt_lower:
            table_matches.append(table_name)
            debug_logs.append(f"✅ Matched '{table_name}' from KG in prompt.")
        else:
            debug_logs.append(f"⛔ Skipped '{table_name}' — not found in prompt.")

    return list(set(table_matches)), debug_logs
