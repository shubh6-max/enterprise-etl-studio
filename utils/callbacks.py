# utils/callbacks.py

from langchain.callbacks import tracing_v2_enabled

def enable_langsmith(project_name: str = "Enterprise ETL Studio"):
    """
    Use this to wrap any LangGraph or LLM agent logic with LangSmith tracing.
    Example:
        with enable_langsmith():
            result = graph.invoke(state)
    """
    return tracing_v2_enabled(project_name=project_name)
