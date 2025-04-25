from typing import Dict, Any
from utils.llm_provider import model  # Uses your LLM wrapper
import json

AGENT_STATE_HINT = {
    "requires": ["raw_prompt", "resolved_tables", "raw_metadata", "sample_records"],
    "produces": ["refined_sql_prompt"]
}

def RefinedPromptBuilderAgent():
    def invoke(state: Dict[str, Any]) -> Dict[str, Any]:
        print("🟣 RefinedPromptBuilderAgent invoked")

        raw_prompt = state.get("raw_prompt", "")
        resolved_tables = state.get("resolved_tables", {})
        raw_metadata = state.get("raw_metadata", {})
        sample_records = state.get("sample_records", {})

        prompt = f"""
You are an expert data analyst.

Your job is to refine a raw business problem into a clearer and well-scoped task 
for SQL logic generation. You are given:
- ✅ The original business problem
- ✅ The resolved Snowflake tables
- ✅ The raw metadata (columns and types) for each table
- ✅ Sample records from each table

Use this to:
- Rephrase the goal into a well-structured prompt
- Include insights based on metadata and samples if needed
- Prepare a clean natural language instruction (NO SQL!)

Respond with only the final refined instruction.

---

🧠 Business Problem:
{raw_prompt.strip()}

📂 Resolved Tables:
{", ".join([f"{k} → {v}" for k, v in resolved_tables.items()])}

📊 Metadata Snapshot (raw):
{raw_metadata}

📈 Sample Records:
{sample_records}
""".strip()

        try:
            response = model.invoke(prompt)
            final_prompt = response.content.strip()
        except Exception as e:
            print(f"❌ LLM failed to generate refined prompt: {e}")
            final_prompt = raw_prompt.strip()  # fallback

        state["refined_sql_prompt"] = final_prompt
        state["chatbot_messages"].append({
            "sender": "assistant",
            "text": f"🧠 Refined SQL prompt ready:\n\n```\n{final_prompt}\n```"
        })

        # 🔽 Save state to test.json
        try:
            with open("test.json", "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2, default=str)
            print("💾 State saved to test.json")
        except Exception as e:
            print(f"❌ Failed to save state: {e}")

        return state

    return invoke
