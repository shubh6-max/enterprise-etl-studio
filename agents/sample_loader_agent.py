import os
import pandas as pd
from typing import Dict
from utils.snowflake_utils import get_snowflake_connection

class SampleLoaderAgent:
    """
    Loads the top 5 records from each selected table (from local or Snowflake).
    """

    def __init__(self, max_rows: int = 5):
        self.max_rows = max_rows

    def __call__(self, state: Dict) -> Dict:
        print("\n🔵 [SampleLoaderAgent] - Loading sample data")

        ds = state.get("data_source", {})
        source_type = ds.get("type")
        tables = ds.get("tables", [])

        if source_type == "local":
            folder_path = ds.get("folder_path")
            return self._load_from_csv(folder_path, tables, state)
        elif source_type == "snowflake":
            database = ds.get("database")
            schema = ds.get("schema")
            return self._load_from_snowflake(database, schema, tables, state)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")

    def _load_from_csv(self, folder_path: str, tables: list, state: Dict) -> Dict:
        sample_data = {}

        for table in tables:
            file_path = os.path.join(folder_path, f"{table}.csv")
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path, nrows=self.max_rows)
                    sample_data[table] = df.to_dict(orient="records")
                except Exception as e:
                    sample_data[table] = f"❌ Failed to load: {str(e)}"
            else:
                sample_data[table] = "❌ File not found"

        return {
            **state,
            "sample_data": sample_data,
            "current_step": "sample_loader"
        }

    def _load_from_snowflake(self, database: str, schema: str, tables: list, state: Dict) -> Dict:
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        sample_data = {}

        for table in tables:
            try:
                query = f'SELECT * FROM "{database.upper()}"."{schema.upper()}"."{table.upper()}" LIMIT {self.max_rows}'
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sample_data[table] = [dict(zip(columns, row)) for row in rows]
            except Exception as e:
                sample_data[table] = f"❌ Failed to load: {str(e)}"

        cursor.close()
        conn.close()

        return {
            **state,
            "sample_data": sample_data,
            "current_step": "sample_loader"
        }
