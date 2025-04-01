import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )

def get_snowflake_tables(database: str, schema: str):
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    query = f"""
    SELECT TABLE_NAME
    FROM {database}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{schema.upper()}'
    """

    cursor.execute(query)
    result = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return result


