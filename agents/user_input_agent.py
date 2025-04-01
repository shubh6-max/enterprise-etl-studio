import os
from typing import Dict
from utils.snowflake_utils import get_snowflake_tables

class UserInputAgent:
    """
    Supports:
    - Local CSV folder: asks for folder, lists files, lets user choose tables
    - Snowflake: asks for DB + schema, lists tables, lets user choose
    """

    def __init__(self, input_mode="cli", source_type="local"):
        self.input_mode = input_mode
        self.source_type = source_type

    def __call__(self, state: Dict) -> Dict:
        print("\n🟡 [UserInputAgent] - Collecting dataset info")

        if self.source_type == "local":
            return self._handle_local(state)
        elif self.source_type == "snowflake":
            return self._handle_snowflake(state)
        else:
            raise ValueError(f"Unsupported source_type: {self.source_type}")

    def _handle_local(self, state: Dict) -> Dict:
        if self.input_mode == "streamlit":
            config = state.get("user_input_config", {})
            folder_path = config.get("folder_path")
            tables = config.get("tables", [])
            csv_files = config.get("available_files", [])

            if not folder_path or not tables:
                raise ValueError("Missing folder_path or tables in user_input_config (streamlit mode)")

            return {
                **state,
                "data_source": {
                    "type": "local",
                    "folder_path": folder_path,
                    "tables": tables,
                    "available_files": csv_files
                },
                "current_step": "user_input"
            }

        # Original CLI mode (unchanged)
        folder_path = input("📁 Enter folder path: ").strip()
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")

        files = os.listdir(folder_path)
        csv_files = [f for f in files if f.endswith(".csv")]

        print("\n📦 Found CSV files:")
        for f in csv_files:
            print(f" - {f}")

        tables_input = input("\n📋 Enter filenames to use as tables (without .csv, comma-separated): ").strip()
        tables = [t.strip() for t in tables_input.split(",") if t.strip()]

        return {
            **state,
            "data_source": {
                "type": "local",
                "folder_path": folder_path,
                "tables": tables,
                "available_files": csv_files
            },
            "current_step": "user_input"
        }


    def _handle_snowflake(self, state: Dict) -> Dict:
        if self.input_mode == "streamlit":
            config = state.get("user_input_config", {})
            db = config.get("database")
            schema = config.get("schema")
            selected_tables = config.get("tables", [])
            all_tables = config.get("available_tables", [])

            if not db or not schema or not selected_tables:
                raise ValueError("Missing DB/schema/tables in user_input_config (streamlit mode)")

            return {
                **state,
                "data_source": {
                    "type": "snowflake",
                    "database": db,
                    "schema": schema,
                    "tables": selected_tables,
                    "available_tables": all_tables
                },
                "current_step": "user_input"
            }

        # Original CLI mode (unchanged)
        db = input("🔷 Enter Snowflake DATABASE: ").strip()
        schema = input("📁 Enter SCHEMA: ").strip()
        all_tables = get_snowflake_tables(db, schema)

        print("\n📦 Tables found in Snowflake:")
        for t in all_tables:
            print(f" - {t}")

        selected = input("\n📋 Enter table names to use (comma-separated): ").strip()
        selected_tables = [t.strip() for t in selected.split(",") if t.strip()]

        return {
            **state,
            "data_source": {
                "type": "snowflake",
                "database": db,
                "schema": schema,
                "tables": selected_tables,
                "available_tables": all_tables
            },
            "current_step": "user_input"
        }

