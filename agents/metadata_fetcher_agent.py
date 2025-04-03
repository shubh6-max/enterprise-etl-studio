import os
import json
import pandas as pd
from typing import Dict
from snowflake.connector.pandas_tools import write_pandas
from utils.snowflake_utils import get_snowflake_connection
from utils.llm_provider import model
from typing import Tuple



class MetadataFetcherAgent:
    def __init__(self):
        pass

    def load_sample_data(self, folder_path: str, table: str, n: int = 5) -> pd.DataFrame:
        return pd.read_csv(os.path.join(folder_path, f"{table}.csv"), nrows=n)

    def load_full_data(self, folder_path: str, table: str) -> pd.DataFrame:
        return pd.read_csv(os.path.join(folder_path, f"{table}.csv"))

    def build_prompt_for_table(self, table_name: str, sample_df: pd.DataFrame) -> str:
        sample_records = sample_df.to_dict(orient="records")
        prompt = f"""
You are a senior data engineer.

Below is a sample of a table named '{table_name}' with 5 rows of data. Your task is to:
1. Suggest enterprise-grade table name (use snake_case with suffix like _dim or _fact).
2. Suggest enterprise-grade column names (use snake_case).
3. Suggest appropriate SQL data types for each column.
4. Return result in JSON with this format ONLY:

{{
  "suggested_table_name": "customer_dim",
  "columns": [
    {{ "original_column": "cust_id", "column_name": "customer_id", "data_type": "NUMBER" }},
    ...
  ]
}}

### Sample Records:
{json.dumps(sample_records, indent=2)}
"""
        return prompt.strip()

    def generate_create_sql(self, suggestion_json: str, database: str, schema: str) -> Tuple[str, str, dict]:
        try:
            parsed = json.loads(suggestion_json)
            suggested_table_name = parsed["suggested_table_name"]
            columns = parsed["columns"]
        except Exception as e:
            raise ValueError(f"❌ Failed to parse LLM metadata: {str(e)}\nRaw:\n{suggestion_json}")

        column_defs = [f'"{col["column_name"]}" {col["data_type"]}' for col in columns]
        rename_map = {col["original_column"]: col["column_name"] for col in columns}

        create_sql = f'CREATE OR REPLACE TABLE "{database}"."{schema}"."{suggested_table_name}" (\n  ' + ",\n  ".join(column_defs) + "\n);"

        return suggested_table_name, create_sql, rename_map

    def __call__(self, state: Dict) -> Dict:
        folder_path = state["data_source"]["folder_path"]
        tables = state["data_source"]["tables"]
        database = state.get("user_input_config", {}).get("database", "")
        schema = state.get("user_input_config", {}).get("schema", "")

        if not database or not schema:
            raise ValueError("❌ Missing database or schema in user_input_config")

        sample_data = {}
        llm_responses = {}
        create_sqls = {}
        upload_report = {}
        enterprise_metadata = {}


        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema}")

        for table in tables:
            try:
                df_sample = self.load_sample_data(folder_path, table)
                prompt = self.build_prompt_for_table(table, df_sample)
                response = model.invoke(prompt)
                llm_output = (
                    response.content.strip()
                    .replace("```json", "")
                    .replace("```", "")
                    .strip()
                    )


                # Debug print (optional)
                print(f"\n🧠 LLM Output for {table}:\n{llm_output}")

                suggested_table, create_sql, rename_map = self.generate_create_sql(llm_output, database, schema)

                # ✅ NEW: Collect column metadata
                parsed = json.loads(llm_output)
                columns = parsed.get("columns", [])
                enterprise_metadata[suggested_table] = [
                    {"column_name": col["column_name"], "data_type": col["data_type"]}
                    for col in columns
                ]


                # Drop & Create table
                cursor.execute(f'DROP TABLE IF EXISTS \"{database}\".\"{schema}\".\"{suggested_table}\"')
                cursor.execute(create_sql)
                conn.commit()


                print(f"📄 Executing CREATE SQL:\n{create_sql}")


                # Rename columns in full data
                df_full = self.load_full_data(folder_path, table)
                df_transformed = df_full.rename(columns=rename_map)

                # Upload
                success, nchunks, nrows, _ = write_pandas(
                    conn=conn,
                    df=df_transformed,
                    table_name=suggested_table,
                    schema=schema,
                    database=database,
                    overwrite=False
                                        )


                sample_data[suggested_table] = df_transformed.head(5).to_dict(orient="records")
                llm_responses[suggested_table] = llm_output
                create_sqls[suggested_table] = create_sql
                upload_report[suggested_table] = f"✅ Uploaded {nrows} rows" if success else "❌ Upload failed"

            except Exception as e:
                upload_report[table] = f"❌ Error: {str(e)}"

        cursor.close()
        conn.close()

        # Update tables with transformed names
        table_name_mapping = {
            original: suggested
            for original, suggested in zip(tables, sample_data.keys())
        }

        return {
            **state,
            "sample_data": sample_data,
            "llm_column_suggestions": llm_responses,
            "llm_create_sql": create_sqls,
            "llm_upload_report": upload_report,
            "enterprise_metadata": enterprise_metadata,
            "table_name_mapping": table_name_mapping,
            "current_step": "metadata_fetcher",

            "output": {
            "sample_data": sample_data,
            "llm_suggestions": llm_responses,
            "create_sqls": create_sqls,
            "upload_status": upload_report,
            "enterprise_metadata": enterprise_metadata,
            "table_name_mapping": table_name_mapping
        },
        "step_outputs": {
            **state.get("step_outputs", {}),
            "metadata_fetcher": {
            "sample_data": sample_data,
            "llm_suggestions": llm_responses,
            "create_sqls": create_sqls,
            "upload_status": upload_report,
            "enterprise_metadata": enterprise_metadata,
            "table_name_mapping": table_name_mapping
            }
        }

    }

