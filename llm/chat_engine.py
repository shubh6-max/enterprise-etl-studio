from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
import os
from dotenv import load_dotenv

# Load API key from .env
load_dotenv()

groq_api_key = os.getenv("GROQ_API_KEY")

# Initialize Groq Chat with LLaMA 3.3 70B Versatile
llm = ChatGroq(
    model="llama-3.3-70b-versatile",
    api_key=groq_api_key,
    temperature=0.3
)

# Updated system prompt for precise and friendly communication
SYSTEM_PROMPT = (
    "You are a precise and intelligent AI assistant helping a user step-by-step in building an ETL workflow. "
    "Speak clearly and concisely, avoiding unnecessary questions. Be friendly but stay focused. "
    "Only ask what's needed at each stage. Confirm user decisions before proceeding."
)

def generate_response(history, user_input):
    """
    Generates a chat response using LLM given a chat history and user input.
    :param history: List of dicts with 'role': 'user' or 'ai', and 'content'
    :param user_input: Latest user input string
    :return: String LLM response
    """
    messages = [SystemMessage(content=SYSTEM_PROMPT)]

    for msg in history:
        if msg["role"] == "user":
            messages.append(HumanMessage(content=msg["content"]))
        elif msg["role"] == "ai":
            messages.append(AIMessage(content=msg["content"]))

    messages.append(HumanMessage(content=user_input))

    response = llm.invoke(messages)
    return response.content
