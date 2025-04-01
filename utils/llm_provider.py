# import os
# from langchain_together import ChatTogether
# from dotenv import load_dotenv

# load_dotenv()

# llm = ChatTogether(
#     model="meta-llama/Llama-3.3-70B-Instruct-Turbo",  # Or "meta-llama/Llama-3-70b-chat-hf"
#     temperature=0.1,
#     max_tokens=1024,
#     together_api_key=os.getenv("TOGETHER_API_KEY"),
# )


# Initialize the Groq LLM using the LLaMA3 70B model with 8192 token context window
import os
from dotenv import load_dotenv

from langchain.chat_models import ChatOpenAI
from langchain_community.chat_models import ChatAnthropic
from langchain_together import ChatTogether
from langchain_groq import ChatGroq  # Groq LLM wrapper for LangChain
from langchain_community.chat_models import ChatFireworks

load_dotenv()

# 🔁 LLM SWITCH CONFIGURATION
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "groq").lower()  # Options: groq, together, anthropic, openai, fireworks
# LLM_MODEL = os.getenv("LLM_MODEL", "mixtral-8x7b-32768")  # Model name override

LLM_MODEL_ChatGroq= "llama-3.3-70b-versatile"     # deepseek-r1-distill-llama-70b
LLM_MODEL_ChatTogether= "meta-llama/Llama-3.3-70B-Instruct-Turbo"
LLM_MODEL_ChatAnthropic= "claude-3-7-sonnet-20250219"
LLM_MODEL_ChatOpenAI= "gpt-3.5-turbo"
LLM_MODEL_ChatFireworks= "accounts/fireworks/models/deepseek-v3-0324"




# 🔌 GROQ
if LLM_PROVIDER == "groq":
    model = ChatGroq(
        model=LLM_MODEL_ChatGroq,  # e.g., "mixtral-8x7b-32768"
        temperature=0.2,
        api_key=os.getenv("GROQ_API_KEY")
    )

# 🤖 TOGETHER
elif LLM_PROVIDER == "together":
    model = ChatTogether(
        model=LLM_MODEL_ChatTogether,  # e.g., "meta-llama/Llama-3.3-70B-Instruct-Turbo"
        temperature=0.2,
        together_api_key=os.getenv("TOGETHER_API_KEY")
    )

# 🔵 ANTHROPIC
elif LLM_PROVIDER == "anthropic":
    model = ChatAnthropic(
        model=LLM_MODEL_ChatAnthropic,  # e.g., "claude-3-opus-20240229"
        temperature=0.2,
        api_key=os.getenv("ANTHROPIC_API_KEY")
    )

# 🔓 OPENAI
elif LLM_PROVIDER == "openai":
    model = ChatOpenAI(
        model=LLM_MODEL_ChatOpenAI,  # e.g., "gpt-3.5-turbo"
        temperature=0.2,
        api_key=os.getenv("OPENAI_API_KEY")
    )

# 🚀 FIREWORKS
elif LLM_PROVIDER == "fireworks":
    model = ChatFireworks(
        model=LLM_MODEL_ChatFireworks,  # e.g., "accounts/fireworks/models/deepseek-r1"
        temperature=0.2,
        api_key=os.getenv("FIREWORKS_API_KEY")
    )

else:
    raise ValueError(f"Unsupported LLM provider: {LLM_PROVIDER}")
