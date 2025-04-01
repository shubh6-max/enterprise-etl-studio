import os
from typing import Dict

class MetadataFetcherAgent:
    """
    Scans a local folder and lists all available data files.
    Verifies presence of expected tables (e.g., Customer.csv).
    """

    def __call__(self, state: Dict) -> Dict:
        print("\n🔵 [MetadataFetcherAgent] - Scanning folder for metadata")

        folder_path = state.get("data_source", {}).get("folder_path")
        expected_tables = state.get("data_source", {}).get("tables", [])

        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")

        files = os.listdir(folder_path)
        csv_files = [f for f in files if f.endswith(".csv")]

        matched = []
        missing = []

        for table in expected_tables:
            expected_file = f"{table}.csv"
            if expected_file in csv_files:
                matched.append(expected_file)
            else:
                missing.append(expected_file)

        result = {
            "folder_path": folder_path,
            "all_files": csv_files,
            "matched_tables": matched,
            "missing_tables": missing
        }

        return {
            **state,
            "metadata": result,
            "current_step": "metadata_fetcher"
        }
