# core/state_schema.py

from typing import Dict, Any, Optional
from datetime import datetime

StateSchema = Dict[str, Any]

def get_initial_state(raw_prompt: str, extra_fields: Optional[Dict[str, Any]] = None) -> StateSchema:
    base_state = {
        "chatbot_messages": [],
        "raw_prompt": raw_prompt,
        "input_mode": "chat",
        "workflow_status": "in_progress",
        "final_timestamp": datetime.now().isoformat(),

        "user_prompt": None,
        "mentioned_tables": None,
        "output_table_name": None,
        "selected_database": None,
        "selected_schema": None,
        "awaiting_user_confirmation": False,
        "user_confirmation": None,
        "task_schedule": None,
        "task_sql": None,
        # ✅ Removed raw_metadata & standardized_metadata here
    }

    if extra_fields:
        base_state.update(extra_fields)

    return base_state
