from langgraph.graph import StateGraph, END

from agents.user_input_agent import UserInputAgent
from agents.user_approval_agent import UserApprovalAgent
from agents.sample_loader_agent import SampleLoaderAgent
from agents.sql_logic_builder_agent import SQLLogicBuilderAgent
from agents.sop_validator_agent import SOPValidatorAgent
from agents.scheduler_agent import SchedulerAgent
from agents.csv_uploader_agent import CSVUploaderAgent


from typing import Dict, Any
builder = StateGraph(Dict[str, Any])


builder.add_node("user_input", UserInputAgent())
builder.add_node("csv_upload", CSVUploaderAgent())
builder.add_node("load_sample_data", SampleLoaderAgent())
builder.add_node("generate_sql", SQLLogicBuilderAgent())
builder.add_node("sql_approval", UserApprovalAgent(approval_for="sql_logic"))
builder.add_node("validate_sop", SOPValidatorAgent())
builder.add_node("sop_approval", UserApprovalAgent(approval_for="sop_sql"))
builder.add_node("schedule_task", SchedulerAgent())
builder.add_node("complete", lambda state: state)

# Edges / Transitions
builder.set_entry_point("user_input")
builder.add_edge("user_input", "csv_upload")
builder.add_edge("csv_upload", "load_sample_data")
builder.add_edge("load_sample_data", "generate_sql")
builder.add_edge("generate_sql", "sql_approval")

builder.add_conditional_edges(
    "sql_approval",
    lambda state: "generate_sql" if state["approval_status"] != "approved" else "validate_sop"
)

builder.add_edge("validate_sop", "sop_approval")
builder.add_conditional_edges(
    "sop_approval",
    lambda state: "validate_sop" if state["approval_status"] != "approved" else "schedule_task"
)

builder.add_edge("schedule_task", "complete")

graph = builder.compile()

print(graph.get_graph().draw_mermaid())