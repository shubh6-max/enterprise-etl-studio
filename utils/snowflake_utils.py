import snowflake.connector
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# --- Connection ---
def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        authenticator=os.getenv("authenticator")
    )

# --- Describe a single table ---
def describe_table_full(table_fqdn: str) -> list[dict]:
    """
    DESCRIBE full metadata for a Snowflake table.
    Input: "DATABASE.SCHEMA.TABLE"
    Output: List of dicts like [{ name: "col", type: "NUMBER", nullable: "Y", comment: "" }, ...]
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"DESC TABLE {table_fqdn.upper()}")
        columns = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        metadata = [dict(zip(column_names, col)) for col in columns]
        return metadata

    except Exception as e:
        print(f"❌ Failed to describe table {table_fqdn}: {e}")
        return []

    finally:
        cursor.close()
        conn.close()

# --- Parallel table metadata fetch ---
def describe_tables_parallel(resolved_tables: dict[str, str]) -> dict[str, list[dict]]:
    """
    Fetches metadata for multiple tables in parallel using threads.
    Input: {"short_name": "DB.SCHEMA.TABLE", ...}
    Output: {"short_name": [metadata], ...}
    """
    result = {}

    def fetch(short_name, fqdn):
        try:
            return short_name, describe_table_full(fqdn)
        except Exception as e:
            print(f"⚠️ Error fetching {fqdn}: {e}")
            return short_name, []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch, short, fqdn) for short, fqdn in resolved_tables.items()]
        for future in as_completed(futures):
            short_name, metadata = future.result()
            result[short_name] = metadata

    return result

# --- Get tables from a specific schema ---
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

# --- Cached: List all tables in the account ---
def list_all_tables() -> list:
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    all_tables = []

    cursor.execute("SHOW DATABASES")
    databases = [row[1] for row in cursor.fetchall()]

    for db in databases:
        try:
            cursor.execute(f"SHOW SCHEMAS IN DATABASE {db}")
            schemas = [row[1] for row in cursor.fetchall()]

            for schema in schemas:
                try:
                    cursor.execute(f"""
                        SELECT TABLE_NAME FROM {db}.INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = '{schema}'
                    """)
                    for row in cursor.fetchall():
                        all_tables.append({
                            "database": db,
                            "schema": schema,
                            "name": row[0]
                        })
                except:
                    continue
        except:
            continue

    cursor.close()
    conn.close()
    return all_tables

# --- Cached: Get all DBs and schemas ---

def get_all_databases_and_schemas() -> dict:
    """
    Returns a dict of all databases and their schemas:
    {
        "MY_DB_1": ["SCHEMA_A", "SCHEMA_B"],
        "MY_DB_2": ["PUBLIC", "RAW"]
    }
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    result = {}

    try:
        cursor.execute("SHOW DATABASES")
        dbs = [row[1] for row in cursor.fetchall()]

        for db in dbs:
            try:
                cursor.execute(f"SHOW SCHEMAS IN DATABASE {db}")
                schemas = [row[1] for row in cursor.fetchall()]
                result[db] = schemas
            except:
                continue
    finally:
        cursor.close()
        conn.close()

    return result
