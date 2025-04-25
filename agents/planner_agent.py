from typing import Dict, Any
from core.state_schema import StateSchema

# Import agent state hints
from agents.input_understanding_agent import AGENT_STATE_HINT as input_understanding_hint
from agents.user_confirmation_agent import AGENT_STATE_HINT as confirmation_hint
from agents.metadata_fetcher_agent import AGENT_STATE_HINT as metadata_fetcher_hint
from agents.post_metadata_confirmation_agent import AGENT_STATE_HINT as post_metadata_confirmation_hint
from agents.sample_loader_agent import AGENT_STATE_HINT as sample_loader_hint
from agents.post_sample_confirmation_agent import AGENT_STATE_HINT as post_sample_confirmation_hint
# from agents.refined_prompt_builder_agent import AGENT_STATE_HINT as refined_prompt_builder_hint
from agents.sql_logic_builder_agent import AGENT_STATE_HINT as sql_logic_builder_hint
from agents.sop_validator_agent import AGENT_STATE_HINT as sop_validator_hint
from agents.sql_executor_agent import AGENT_STATE_HINT as sql_executor_hint
from agents.cte_extractor_agent import AGENT_STATE_HINT as cte_extractor_hint
from agents.sql_task_graph_agent import AGENT_STATE_HINT as sql_task_graph_hint


# ✅ All agents registry
AGENT_REGISTRY = {
    "input_understanding_agent": input_understanding_hint,
    "user_confirmation_agent": confirmation_hint,
    "metadata_fetcher_agent": metadata_fetcher_hint,
    "post_metadata_confirmation_agent": post_metadata_confirmation_hint,
    "sample_loader_agent": sample_loader_hint,
    "post_sample_confirmation_agent": post_sample_confirmation_hint,
    # "refined_prompt_builder_agent": refined_prompt_builder_hint,
    "sql_logic_builder_agent": sql_logic_builder_hint,
    "sop_validator_agent": sop_validator_hint,
    "sql_executor_agent": sql_executor_hint,
    "cte_extractor_agent": cte_extractor_hint,
    "sql_task_graph_agent": sql_task_graph_hint,

}

def planner_router(state: StateSchema) -> str:
    print("\n🧠📍 LangGraph Planner Called")
    print("🧠 Current State Keys:", list(state.keys()))

    # Step 1: Handle initial confirmation
    if state.get("awaiting_user_confirmation", False):
        confirmation = state.get("user_confirmation", "") or ""
        if confirmation.strip().lower() != "yes":
            print("⏳ Still waiting for user confirmation...")
            return "user_confirmation_agent"
        else:
            print("✅ User confirmation received")
            state["awaiting_user_confirmation"] = False    

    # Step 3: Route to agents based on missing outputs
    for agent_name, spec in AGENT_REGISTRY.items():
        required = spec.get("requires", [])
        produced = spec.get("produces", [])

        if all(k in state for k in required) and any(k not in state or state[k] is None for k in produced):
            print(f"✅ Routing to agent: {agent_name}")
            return agent_name

    # Final fallback
    print("✅ No more steps. Workflow complete.")
    return "workflow_complete_agent"
