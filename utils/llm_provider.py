import os
from dotenv import load_dotenv

from langchain.chat_models import ChatOpenAI
from langchain_community.chat_models import ChatOpenAI
from langchain_together import ChatTogether
from langchain_groq import ChatGroq  # Groq LLM wrapper for LangChain
from langchain_community.chat_models import ChatFireworks
from langchain_anthropic import ChatAnthropic


load_dotenv()

# 🔁 LLM SWITCH CONFIGURATION
# LLM_PROVIDER = os.getenv("LLM_PROVIDER", "groq").lower()  # Options: groq, together, anthropic, openai, fireworks
# LLM_MODEL = os.getenv("LLM_MODEL", "mixtral-8x7b-32768")  # Model name override

LLM_MODEL_ChatGroq= "llama-3.3-70b-versatile"    # deepseek-r1-distill-llama-70b   #meta-llama/llama-4-maverick-17b-128e-instruct   "llama-3.3-70b-versatile"
LLM_MODEL_ChatTogether= "meta-llama/Llama-3.3-70B-Instruct-Turbo"
LLM_MODEL_ChatAnthropic= "claude-3-7-sonnet-20250219"
LLM_MODEL_ChatOpenAI= "gpt-3.5-turbo"
LLM_MODEL_ChatFireworks= "accounts/fireworks/models/deepseek-v3-0324"

def get_llm():
    provider = os.getenv("LLM_PROVIDER_groq", "together").lower()

    if provider == "groq":
        return ChatGroq(
            temperature=0,
            model_name= LLM_MODEL_ChatGroq,
            api_key=os.getenv("GROQ_API_KEY_1")
        )

    elif provider == "together":
        return ChatTogether(
            temperature=0,
            model= LLM_MODEL_ChatTogether,  # example
            api_key=os.getenv("TOGETHER_API_KEY")
        )

    elif provider == "fireworks":
        return ChatFireworks(
            temperature=0,
            model= LLM_MODEL_ChatFireworks,
            api_key=os.getenv("FIREWORKS_API_KEY")
        )

    elif provider == "openai":
        return ChatOpenAI(
            temperature=0,
            model= LLM_MODEL_ChatOpenAI,
            api_key=os.getenv("OPENAI_API_KEY")
        )

    elif provider == "anthropic":
        return ChatAnthropic(
            temperature=0,
            model= LLM_MODEL_ChatAnthropic,  # or haiku/sonnet
            api_key=os.getenv("ANTHROPIC_API_KEY")
        )

    else:
        raise ValueError(f"❌ Unsupported LLM_PROVIDER: {provider}")

# Optional default
model = get_llm()
