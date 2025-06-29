"""Utilities to assemble the ETL LangGraph workflow.

This module wires up all the available agents into a :class:`StateGraph` and
routes between them using the planner.  The goal is to keep the graph
construction in one place and make it easy to modify the workflow order.
"""

from typing import Callable, Dict

from langgraph.graph import END, StateGraph

from core.state_schema import StateSchema
from agents.planner_agent import planner_router
from agents.input_understanding_agent import InputUnderstandingAgent
from agents.user_confirmation_agent import UserConfirmationAgent
from agents.metadata_fetcher_agent import MetadataFetcherAgent
from agents.post_metadata_confirmation_agent import (
    PostMetadataConfirmationAgent,
)
from agents.sample_loader_agent import SampleLoaderAgent
from agents.post_sample_confirmation_agent import PostSampleConfirmationAgent
# from agents.refined_prompt_builder_agent import RefinedPromptBuilderAgent
from agents.sql_logic_builder_agent import SQLLogicBuilderAgent
from agents.sop_validator_agent import SOPValidatorAgent
from agents.sql_executor_agent import SQLExecutorAgent
from agents.cte_extractor_agent import CTEExtractorAgent
from agents.sql_task_graph_agent import SQLTaskGraphAgentInvoke

def workflow_complete_agent(state: StateSchema) -> StateSchema:
    state["workflow_status"] = "completed"
    state["chatbot_messages"].append({
        "sender": "assistant",
        "text": "✅ Workflow completed successfully!"
    })
    return state

# Map agent name to the callable that creates the agent.
AGENT_CONSTRUCTORS: Dict[str, Callable[[], Callable]] = {
    "input_understanding_agent": InputUnderstandingAgent,
    "user_confirmation_agent": UserConfirmationAgent,
    "metadata_fetcher_agent": MetadataFetcherAgent,
    "post_metadata_confirmation_agent": PostMetadataConfirmationAgent,
    "sample_loader_agent": SampleLoaderAgent,
    "post_sample_confirmation_agent": PostSampleConfirmationAgent,
    # "refined_prompt_builder_agent": RefinedPromptBuilderAgent,
    "sql_logic_builder_agent": SQLLogicBuilderAgent,
    "sop_validator_agent": SOPValidatorAgent,
    "sql_executor_agent": SQLExecutorAgent,
    "cte_extractor_agent": CTEExtractorAgent,
    "sql_task_graph_agent": SQLTaskGraphAgentInvoke,
}

# Order in which agents should be executed. The planner will decide whether an
# agent actually runs based on the current state, but defining the order keeps
# the graph deterministic.
AGENT_ORDER = [
    "input_understanding_agent",
    "user_confirmation_agent",
    "metadata_fetcher_agent",
    "post_metadata_confirmation_agent",
    "sample_loader_agent",
    "post_sample_confirmation_agent",
    # "refined_prompt_builder_agent",
    "sql_logic_builder_agent",
    "sop_validator_agent",
    "sql_executor_agent",
    "cte_extractor_agent",
    "sql_task_graph_agent",
]


def build_etl_graph() -> StateGraph:
    """Compile and return the ETL workflow graph."""

    builder = StateGraph(StateSchema)

    # Add nodes based on AGENT_ORDER
    for name in AGENT_ORDER:
        builder.add_node(name, AGENT_CONSTRUCTORS[name]())

    # Completion node
    builder.add_node("workflow_complete_agent", workflow_complete_agent)

    # Set entry point
    builder.set_entry_point("input_understanding_agent")

    # Set up conditional edges so each agent hands control back to the planner
    all_agents = {
        name: name for name in AGENT_ORDER[1:] + ["workflow_complete_agent"]
    }

    for name in AGENT_ORDER:
        builder.add_conditional_edges(name, planner_router, all_agents)

    # End state
    builder.add_edge("workflow_complete_agent", END)

    return builder.compile()
