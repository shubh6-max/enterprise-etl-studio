from typing import Dict

class UserApprovalAgent:
    """
    Asks user to approve/reject a step, based on current_step + step_outputs.
    """

    def __init__(self, input_mode="cli"):
        self.input_mode = input_mode

    def __call__(self, state: Dict) -> Dict:
        step = state.get("current_step")
        output = state.get("step_outputs", {}).get(step, {})

        print(f"\n🟢 [UserApprovalAgent] - Step: {step}")
        print("🔍 Output to review:")
        print("------------------------------------")
        for k, v in output.items():
            print(f"{k}: {v}")
        print("------------------------------------")

        if self.input_mode == "cli":
            while True:
                decision = input("✅ Approve this step? (yes/no): ").strip().lower()
                if decision in ["yes", "no"]:
                    return {
                        **state,
                        "approval_status": "approved" if decision == "yes" else "rejected"
                    }
                print("⚠️ Please enter 'yes' or 'no'")
        elif self.input_mode == "streamlit":
            approval_config = state.get("approval_config", {})
            decision = approval_config.get(step)  # should be "approved" or "rejected"

            if decision not in ["approved", "rejected"]:
                raise ValueError(f"Missing or invalid approval decision in state['approval_config'] for step: {step}")

            return {
                **state,
                "approval_status": decision
            }
        else:
            raise ValueError(f"Unsupported input_mode: {self.input_mode}")

