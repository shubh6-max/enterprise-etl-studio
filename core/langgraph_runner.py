from langgraph.graph import StateGraph, END
from core.state_schema import StateSchema
from agents.planner_agent import planner_router
from agents.input_understanding_agent import InputUnderstandingAgent
from agents.user_confirmation_agent import UserConfirmationAgent
from agents.metadata_fetcher_agent import MetadataFetcherAgent
from agents.post_metadata_confirmation_agent import PostMetadataConfirmationAgent
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

def build_etl_graph():
    builder = StateGraph(StateSchema)

    # 🔵 Entry node — ONLY runs once
    builder.add_node("input_understanding_agent", InputUnderstandingAgent())
    builder.set_entry_point("input_understanding_agent")

    # 🧠 All other agents
    builder.add_node("user_confirmation_agent", UserConfirmationAgent())
    builder.add_node("metadata_fetcher_agent", MetadataFetcherAgent())
    builder.add_node("post_metadata_confirmation_agent", PostMetadataConfirmationAgent())
    builder.add_node("sample_loader_agent", SampleLoaderAgent())
    builder.add_node("post_sample_confirmation_agent", PostSampleConfirmationAgent())
    # builder.add_node("refined_prompt_builder_agent", RefinedPromptBuilderAgent())
    builder.add_node("sql_logic_builder_agent", SQLLogicBuilderAgent())
    builder.add_node("sop_validator_agent", SOPValidatorAgent())
    builder.add_node("sql_executor_agent", SQLExecutorAgent())
    builder.add_node("cte_extractor_agent", CTEExtractorAgent())
    builder.add_node("sql_task_graph_agent", SQLTaskGraphAgentInvoke())

    builder.add_node("workflow_complete_agent", workflow_complete_agent)

    # 📍 Conditional routing for all agents EXCEPT the entry
    conditional_agents = [
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
        "sql_task_graph_agent"
    ]

    all_agents = {
        agent_name: agent_name for agent_name in conditional_agents + ["workflow_complete_agent"]
    }

    for agent_name in conditional_agents:
        builder.add_conditional_edges(agent_name, planner_router, all_agents)

    # ✅ InputUnderstandingAgent → planner
    builder.add_conditional_edges("input_understanding_agent", planner_router, all_agents)

    # 🛑 End state
    builder.add_edge("workflow_complete_agent", END)

    return builder.compile()
