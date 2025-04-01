import os
import re
import pandas as pd
from typing import Dict
from snowflake.connector.pandas_tools import write_pandas
from utils.snowflake_utils import get_snowflake_connection


class CSVUploaderAgent:
    def __init__(self, input_mode="cli"):
        self.input_mode = input_mode

    def __call__(self, state: Dict) -> Dict:
        print("\n📤 [CSVUploaderAgent] - Uploading local CSVs to Snowflake using write_pandas()")

        if state.get("data_source", {}).get("type") != "local":
            print("⚠️ Not a local source. Skipping upload.")
            return state

        folder_path = state["data_source"]["folder_path"]
        tables = state["data_source"]["tables"]

        conn = get_snowflake_connection()
        cursor = conn.cursor()

        # Step 1: Ask user for DB/Schema
        upload_cfg = state.get("csv_upload_config", {})

        if self.input_mode == "streamlit":
            database = upload_cfg.get("database")
            schema = upload_cfg.get("schema")

            if not database or not schema:
                raise ValueError("Missing target DATABASE or SCHEMA in csv_upload_config")

        else:
            print("\n📚 Available Databases:")
            cursor.execute("SHOW DATABASES")
            for db in cursor.fetchall():
                print(f"- {db[1]}")

            database = input("\n📦 Enter target DATABASE: ").strip()
            schema = input("📂 Enter target SCHEMA: ").strip()


        # Set context
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema}")

        # Prepare table mapping
        table_map = {tbl: self._sanitize_table_name(tbl) for tbl in tables}
        print(f"\n🧾 Tables to be created in {database}.{schema}:")
        for k, v in table_map.items():
            print(f"- {k}.csv → {v}")

        if self.input_mode == "streamlit":
            approved = upload_cfg.get("approved", False)
            if not approved:
                print("❌ Upload cancelled.")
                return {**state, "upload_status": {"status": "cancelled"}, "current_step": "csv_uploader"}
        else:
            approve = input("✅ Proceed with upload? (yes/no): ").strip().lower()
            if approve != "yes":
                print("❌ Upload cancelled.")
                return {**state, "upload_status": {"status": "cancelled"}, "current_step": "csv_uploader"}


        upload_report = {}

        for original in tables:
            cleaned = table_map[original]
            path = os.path.join(folder_path, f"{original}.csv")

            try:
                df = pd.read_csv(path)
                total_rows = len(df)
                print(f"\n⬆️ Uploading '{original}' ({total_rows} rows) → {cleaned}")

                # Drop table if exists
                cursor.execute(f'DROP TABLE IF EXISTS "{cleaned}"')

                # Upload using write_pandas
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df,
                    table_name=cleaned,
                    schema=schema,
                    database=database,
                    overwrite=True
                )

                if success:
                    print(f"✅ {nrows} rows uploaded to {cleaned}")
                    upload_report[original] = f"✅ Uploaded ({nrows} rows)"
                else:
                    print(f"❌ Upload failed for {cleaned}")
                    upload_report[original] = f"❌ Failed to upload"

            except Exception as e:
                print(f"❌ Upload failed for {cleaned}: {e}")
                upload_report[original] = f"❌ Failed: {str(e)}"

        cursor.close()
        conn.close()

        return {
            **state,
            "csv_upload": {
                "database": database,
                "schema": schema,
                "table_map": table_map,
                "upload_report": upload_report
            },
            "current_step": "csv_uploader"
        }

    def _sanitize_table_name(self, name: str) -> str:
        name = name.lower()
        name = re.sub(r'[^a-z0-9]+', '_', name)
        return name.upper().strip("_")
