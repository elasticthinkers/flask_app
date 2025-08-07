import os
import json
import time
import ast
import boto3
import yaml
import requests
import psycopg2
import pandas as pd
from io import BytesIO
from flask import Flask, request, jsonify
from datetime import datetime
from psycopg2.extras import RealDictCursor


app = Flask(__name__)

LAMBDA_SECRET_API = "https://pil9maduj9.execute-api.us-east-1.amazonaws.com/default/get_secrets"
secrets=None
# ------------------------ Utility Functions ------------------------

def get_secrets():
    print("📦 Fetching secrets...")
    payload = {
        "key": [
            "FLASK_SERVER", "BEDROCK_MODEL_ID", "AWS_REGION",
            "RDS_DB_HOST", "RDS_DB_PORT", "RDS_DB_NAME", "RDS_DB_USER", "RDS_DB_PASSWORD",
            "NEON_DB_URL", "NOTIFY_AGENT_API", "LOG_AGENT_API", "AWS_S3_BUCKET_NAME","BEDROCK_MODEL_ID","AWS_SESSION_TOKEN",
            "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY","SENDER_MAIL",
        ]
    }
    response = requests.post(LAMBDA_SECRET_API, json=payload)
    response.raise_for_status()
    return response.json()

secrets = get_secrets()


model_id = secrets.get("BEDROCK_MODEL_ID")
region_name = secrets.get("AWS_REGION")

client = boto3.client("bedrock-runtime", region_name=region_name)

AWS_REGION = secrets.get("AWS_REGION")
BUCKET_NAME = secrets.get("AWS_S3_BUCKET_NAME")
BEDROCK_MODEL_ID = secrets.get("BEDROCK_MODEL_ID")
SENDER_MAIL = secrets.get("SENDER_MAIL")
FLASK_SERVER = secrets.get("FLASK_SERVER")
LOG_AGENT_URL = secrets.get("LOG_AGENT_API")
log_api = secrets.get("LOG_AGENT_API")
notify_api = secrets.get("NOTIFY_AGENT_API")
LOG_AGENT_API = secrets.get("LOG_AGENT_API")
NOTIFY_AGENT_API = secrets.get("NOTIFY_AGENT_API")
NOTIFY_AGENT_URL = secrets.get("NOTIFY_AGENT_API")

# AWS clients
s3 = boto3.client('s3', region_name=AWS_REGION)
bedrock = boto3.client('bedrock-runtime', region_name=AWS_REGION)

# S3 Paths
RULES_PATH = "tables/rules/"
RESULTS_PATH = "tables/rules-result/"

DB_CONFIG = {
    'host': secrets["RDS_DB_HOST"],
    'port': int(secrets["RDS_DB_PORT"]),
    'dbname': secrets["RDS_DB_NAME"],
    'user': secrets["RDS_DB_USER"],
    'password': secrets["RDS_DB_PASSWORD"]
}

def connect_to_rds():
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        dbname=DB_CONFIG['dbname'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )
# Function of Validation Agent
def compare_table_schemas(neon_conn_str, rds_config, source_table, target_table):
    def get_table_schema(cursor, table_name):
        query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """
        cursor.execute(query, (table_name,))
        return cursor.fetchall()

    try:
        # Connect to NeonDB
        print("Connecting to NeonDB...")
        neon_conn = psycopg2.connect(neon_conn_str)
        neon_cursor = neon_conn.cursor()
        neon_schema = get_table_schema(neon_cursor, source_table)
        print(f"✅ Retrieved schema for {source_table} from NeonDB")

        # Connect to Amazon RDS
        print("Connecting to Amazon RDS...")
        rds_conn = psycopg2.connect(**rds_config)
        rds_cursor = rds_conn.cursor()
        rds_schema = get_table_schema(rds_cursor, target_table)
        print(f"✅ Retrieved schema for {target_table} from RDS")

        # Compare schema length
        if len(neon_schema) != len(rds_schema):
            msg = f"❌ Mismatch in number of columns: NeonDB has {len(neon_schema)}, RDS has {len(rds_schema)}"
            print(msg)
            return False, msg

        # Compare columns
        mismatch = False
        details = []
        for i in range(len(neon_schema)):
            neon_col, neon_type = neon_schema[i]
            rds_col, rds_type = rds_schema[i]
            if neon_col != rds_col or neon_type != rds_type:
                mismatch = True
                diff = f"❌ Mismatch at column {i+1}: NeonDB: {neon_col} ({neon_type}), RDS: {rds_col} ({rds_type})"
                print(diff)
                details.append(diff)

        if mismatch:
            return False, "\n".join(details)
        
        print("✅ Table schemas are identical")
        return True, "✅ Table schemas are identical"

    except Exception as e:
        print(f"❌ Schema check failed due to exception: {str(e)}")
        return False, f"Exception during schema check: {str(e)}"
    finally:
        if 'neon_cursor' in locals(): neon_cursor.close()
        if 'neon_conn' in locals(): neon_conn.close()
        if 'rds_cursor' in locals(): rds_cursor.close()
        if 'rds_conn' in locals(): rds_conn.close()

def compare_row_counts(neon_conn_str, rds_config, source_table, target_table, primary_keys):
    print("⚙️ Connecting to both databases for row count check...")

    try:
        # Connect to NeonDB
        print("Connecting to NeonDB...")
        neon_conn = psycopg2.connect(neon_conn_str)
        neon_cursor = neon_conn.cursor(cursor_factory=RealDictCursor)
        print(f"✅ NeonDB connected")

        # Connect to Amazon RDS
        print("Connecting to Amazon RDS...")
        rds_conn = psycopg2.connect(**rds_config)
        rds_cursor = rds_conn.cursor(cursor_factory=RealDictCursor)
        print(f"✅ RDS connected")

        # Step 1: Count rows
        neon_cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
        neon_count = neon_cursor.fetchone()['count']

        rds_cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        rds_count = rds_cursor.fetchone()['count']

        print(f"📊 NeonDB count: {neon_count}, RDS count: {rds_count}")

        if neon_count == rds_count:
            print("✅ Row counts match.")
            return True, "✅ Row counts match"

        # Step 2: Fetch all PKs
        print("🔍 Row count mismatch. Checking primary key differences...")
        pk_columns = ', '.join(primary_keys)

        neon_cursor.execute(f"SELECT {pk_columns} FROM {source_table}")
        neon_rows = neon_cursor.fetchall()

        rds_cursor.execute(f"SELECT {pk_columns} FROM {target_table}")
        rds_rows = rds_cursor.fetchall()

        neon_pks = {tuple(row[pk] for pk in primary_keys) for row in neon_rows}
        rds_pks = {tuple(row[pk] for pk in primary_keys) for row in rds_rows}

        missing_pks = neon_pks - rds_pks
        extra_pks = rds_pks - neon_pks

        issues = []
        if missing_pks:
            issues.append(f"❌ Missing rows in RDS: {len(missing_pks)}")
            for pk_tuple in list(missing_pks)[:5]:  # show only first 5
                pk_condition = " AND ".join(f"{pk} = %s" for pk in primary_keys)
                neon_cursor.execute(
                    f"SELECT * FROM {source_table} WHERE {pk_condition}",
                    pk_tuple
                )
                row = neon_cursor.fetchone()
                issues.append(f"🔸 Missing PK: {pk_tuple} → Row: {row}")
        
        if extra_pks:
            issues.append(f"⚠️ Extra rows in RDS not in NeonDB: {len(extra_pks)}")
            for pk_tuple in list(extra_pks)[:5]:
                pk_condition = " AND ".join(f"{pk} = %s" for pk in primary_keys)
                rds_cursor.execute(
                    f"SELECT * FROM {target_table} WHERE {pk_condition}",
                    pk_tuple
                )
                row = rds_cursor.fetchone()
                issues.append(f"🔸 Extra PK: {pk_tuple} → Row: {row}")

        full_detail = "\n".join(issues)
        return False, full_detail

    except Exception as e:
        return False, f"❌ Exception during row count comparison: {str(e)}"

    finally:
        neon_cursor.close()
        rds_cursor.close()
        neon_conn.close()
        rds_conn.close()

def compare_row_data(neon_conn_str, rds_config, table_name, primary_keys):
    try:
        # Connect to both databases
        # Connect to NeonDB
        print("Connecting to NeonDB...")
        neon_conn = psycopg2.connect(neon_conn_str)
        print(f"✅ NeonDB connected")

        # Connect to Amazon RDS
        print("Connecting to Amazon RDS...")
        rds_conn = psycopg2.connect(**rds_config)
        print(f"✅ RDS connected")

        print("📥 Loading table data into DataFrames...")
        neon_df = pd.read_sql(f"SELECT * FROM {table_name}", neon_conn)
        rds_df = pd.read_sql(f"SELECT * FROM {table_name}", rds_conn)
        total_rows = len(neon_df)
        total_columns = len(neon_df.columns)
        volume_count = total_rows * total_columns
        neon_conn.close()
        rds_conn.close()

        # Set composite key
        neon_df.set_index(primary_keys, inplace=True)
        rds_df.set_index(primary_keys, inplace=True)

        common_keys = neon_df.index.intersection(rds_df.index)

        mismatches = []
        total_value_mismatches = 0
        columns_with_mismatches = set()
        print("still running")
        # for key in common_keys:
        #     neon_row = neon_df.loc[key]
        #     rds_row = rds_df.loc[key]

        #     if isinstance(neon_row, pd.Series):
        #         differing_cols = []
        #         for col in neon_df.columns:
        #             val1, val2 = neon_row[col], rds_row[col]
        #             if pd.isna(val1) and pd.isna(val2):
        #                 continue
        #             if val1 != val2:
        #                 differing_cols.append(col)
        #                 columns_with_mismatches.add(col)
        #                 total_value_mismatches += 1

        #         if differing_cols:
        #             mismatches.append({
        #                 "primary_key": list(key),
        #                 "differing_columns": differing_cols
        #             })
        for key in common_keys:
            neon_row = neon_df.loc[key]
            rds_row = rds_df.loc[key]
        
            # Ensure we're working with Series
            if isinstance(neon_row, pd.DataFrame) or isinstance(rds_row, pd.DataFrame):
                print(f"⚠️ Duplicate primary key detected for key: {key}")
                continue
        
            differing_cols = []
            for col in neon_df.columns:
                val1, val2 = neon_row[col], rds_row[col]
                
                if pd.isna(val1) and pd.isna(val2):
                    continue
                if (pd.isna(val1) != pd.isna(val2)) or (val1 != val2):
                    differing_cols.append(col)
                    columns_with_mismatches.add(col)
                    total_value_mismatches += 1
        
            if differing_cols:
                mismatches.append({
                    "primary_key": list(key) if isinstance(key, tuple) else [key],
                    "differing_columns": differing_cols
                })

        print({
            "matched": len(mismatches) == 0,
            "mismatched_rows": mismatches,
            "metrics": {
                "row_count": total_rows,
                "column_count": total_columns,
                "volume_count": volume_count,
                "total_row_mismatches": len(mismatches),
                "total_column_mismatches": len(columns_with_mismatches),
                "total_value_mismatches": total_value_mismatches
            }
        })
        return {
            "matched": len(mismatches) == 0,
            "mismatched_rows": mismatches,
            "metrics": {
                "row_count": total_rows,
                "column_count": total_columns,
                "volume_count": volume_count,
                "total_row_mismatches": len(mismatches),
                "total_column_mismatches": len(columns_with_mismatches),
                "total_value_mismatches": total_value_mismatches
            }
        }

    except Exception as e:
        return {
            "matched": False,
            "error": str(e),
            "mismatched_rows": [],
            "metrics": {
                "total_row_mismatches": 0,
                "total_column_mismatches": 0,
                "total_value_mismatches": 0
            }
        }

def handle_issue(issue_type, run_id, source_table, target_table, result, start_time, notify_api, log_api):
    end_time = datetime.utcnow()
    duration = int((end_time - start_time).total_seconds())
    NOTIFY_AGENT_API
    notify_body = {
        "type": "issue",
        "timestamp": end_time.isoformat() + "Z",
        "agent": "validation-agent",
        "source_table": source_table,
        "target_table": target_table,
        "details": result
    }
    log_body = {
        "run_id": run_id,
        "log_add": {
            "agent": "validation-agent",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "status": "success",
            "result": "failed",
            "details": result
        }
    }
    print("📤 Posting to Notify Agent...",NOTIFY_AGENT_API)
    requests.post(NOTIFY_AGENT_API, json=notify_body)
    print("📝 Logging to Log Agent...")
    requests.post(log_api, json=log_body)
    return jsonify({
        "duration": duration,
        "next_agent": "remediation-agent",
        "issue": issue_type,
        "issue_details": result
    })

# ------------------------ Quality Rule Agent ------------------------

#Function For Quality Rule Agent
# === Evaluate Rule ===
def evaluate_rule_result(rule, result):
    try:
        if 'criteria' in rule and 'value' in rule:
            value = rule['value']
            if value is None:
                return result == 0 if rule['criteria'] == "!=" else result > 0
            if rule['criteria'] == "=":
                return result == value
            elif rule['criteria'] == "!=":
                return result != value
            elif rule['criteria'] == ">":
                return result > value
            elif rule['criteria'] == "<":
                return result < value
            elif rule['criteria'] == ">=":
                return result >= value
            elif rule['criteria'] == "<=":
                return result <= value
        elif rule.get('key') == 'duplicates':
            return result == 0 if rule['criteria'] == "!=" else result > 0
        elif 'data-type' in rule:
            return result[1] == rule['data-type']
        elif 'allowed_values' in rule:
            return result == 0
        elif 'allowed_pattern' in rule:
            return result == 0
        return True
    except:
        return False

# === Generate SQL Query using Bedrock Claude 3.5 with Fallback ===
def generate_query_from_bedrock(rule, table_name):
    if 'key' not in rule:
        return None

    prompt = build_bedrock_prompt(rule, table_name)
    try:
        native_request = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 100000,
            "temperature": 0.1,
            "messages": [
                {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
                }
            ],
        }

        # Convert the native request to JSON.
        request = json.dumps(native_request)

        # Invoke the model with the request.
        response = client.invoke_model(modelId=model_id, body=request)
        model_response = json.loads(response["body"].read())      
        queries = model_response["content"][0]["text"]
        print(f"Generated query: {queries}")
        return queries
    except Exception as e:
        # Fallback query logic
        if rule.get('criteria') in ('!=', '=') and rule.get('key') == 'null':
            return f"SELECT COUNT(*) FROM {table_name} WHERE {rule['key']} IS NULL"
        elif rule.get('key') == 'duplicates':
            return f"SELECT COUNT(*) FROM (SELECT COUNT(*) FROM {table_name} GROUP BY * HAVING COUNT(*) > 1) AS dup"
        elif 'aggregate' in rule:
            return f"SELECT {rule['aggregate']}({rule['key']}) FROM {table_name}"
        elif 'data-type' in rule:
            return f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}' AND column_name = '{rule['key']}'
            """
        elif 'value' in rule:
            val = 'NULL' if rule['value'] is None else f"'{rule['value']}'"
            return f"SELECT COUNT(*) FROM {table_name} WHERE {rule['key']} {rule['criteria']} {val}"
        elif 'allowed_values' in rule:
            allowed = ', '.join([f"'{v}'" for v in rule['allowed_values']])
            return f"SELECT COUNT(*) FROM {table_name} WHERE {rule['key']} NOT IN ({allowed})"
        elif 'allowed_pattern' in rule:
            return f"SELECT COUNT(*) FROM {table_name} WHERE {rule['key']} !~ '{rule['allowed_pattern']}'"
        return None

# === Prompt Builder for Bedrock Claude 3.5 ===
def build_bedrock_prompt(rule, table_name):
    examples = f"""
Example 1:
Rule: {{ "key": "amount", "aggregate": "sum", "criteria": "=", "value": 10000 }}
SQL: SELECT SUM(amount) FROM transactions;

Example 2:
Rule: {{ "key": "status", "criteria": "!=", "value": "inactive" }}
SQL: SELECT COUNT(*) FROM transactions WHERE status != 'inactive';

Example 3:
Rule: {{ "key": "cost", "data-type": "double", "criteria": "=" }}
SQL: SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'transactions' AND column_name = 'cost';

Example 4:
Rule: {{ "key": "transaction_date", "criteria": "!=", "value": null }}
SQL: SELECT COUNT(*) FROM transactions WHERE transaction_date IS NULL;

Example 5:
Rule: {{ "key": "duplicates", "criteria": "!=", "column": "id" }}
SQL: SELECT COUNT(*) FROM (SELECT id, COUNT(*) FROM transactions GROUP BY id HAVING COUNT(*) > 1) AS dup;

Example 6:
Rule: {{ "key": "email", "allowed_pattern": "^[\\\\w.-]+@[\\\\w.-]+\\\\.\\\\w{{2,}}$" }}
SQL: SELECT COUNT(*) FROM transactions WHERE email !~ '^[\\\\w.-]+@[\\\\w.-]+\\\\.\\\\w{{2,}}$';

Example 7:
Rule: {{ "key": "country", "allowed_values": ["USA", "Canada", "UK", "India"] }}
SQL: SELECT COUNT(*) FROM transactions WHERE country NOT IN ('USA', 'Canada', 'UK', 'India');

Example 8:
Rule: {{ "key": "is_active", "allowed_values": [true, false] }}
SQL: SELECT COUNT(*) FROM transactions WHERE is_active NOT IN (true, false);

---

Now generate SQL query for:
Table name: {table_name}
Rule: {json.dumps(rule)}

 Only return SQL. No explanation.
 Do not include starting ```json and trailing ```.
"""
    return examples

#remediation agent
def refresh_rds_with_neondb(source_table, target_table, neon_conn_str, rds_config):
    try:
        # Step 1: Connect to both databases
        neon_conn = psycopg2.connect(neon_conn_str)
        neon_cursor = neon_conn.cursor()
        rds_conn = psycopg2.connect(**rds_config)
        rds_cursor = rds_conn.cursor()

        # Step 2: Drop target table in RDS
        rds_cursor.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE;")
        rds_conn.commit()

        # Step 3: Get schema from Neon
        neon_cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (source_table,))
        schema = neon_cursor.fetchall()

        if not schema:
            return {"status": "fail", "error": "empty_source_schema"}

        # Step 4: Create table in RDS
        column_definitions = ", ".join([f"{col} {dtype}" for col, dtype in schema])
        create_table_sql = f"CREATE TABLE {target_table} ({column_definitions});"
        rds_cursor.execute(create_table_sql)
        rds_conn.commit()

        # Step 5: Copy data from Neon
        neon_cursor.execute(f"SELECT * FROM {source_table};")
        rows = neon_cursor.fetchall()

        if not rows:
            return {"status": "success", "message": "schema refreshed, no data"}

        placeholders = ", ".join(["%s"] * len(schema))
        insert_query = f"INSERT INTO {target_table} VALUES ({placeholders});"
        rds_cursor.executemany(insert_query, rows)
        rds_conn.commit()

        return {"status": "success", "rows_copied": len(rows)}

    except Exception as e:
        print(f"❌ Error during refresh: {str(e)}")
        return {"status": "fail", "error": str(e)}

    finally:
        try:
            neon_cursor.close()
            neon_conn.close()
            rds_cursor.close()
            rds_conn.close()
            print("🔒 Closed all DB connections.")
        except Exception as e:
            print(f"⚠️ Error closing connections: {str(e)}")


def get_table_schema(cursor, table_name):
    print(f"📑 Getting schema for table: {table_name}")
    cursor.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
    """, (table_name,))
    schema = cursor.fetchall()
    print(f"✅ Retrieved schema: {schema}")
    return schema

def refresh_rds_with_neondb(source_table, target_table, neon_conn_str, rds_config):
    print(f"🔄 Starting full refresh of `{target_table}` in RDS using `{source_table}` from Neon...")

    try:
        # Step 1: Connect to both databases
        neon_conn = psycopg2.connect(neon_conn_str)
        neon_cursor = neon_conn.cursor()
        rds_conn = psycopg2.connect(**rds_config)
        rds_cursor = rds_conn.cursor()

        # Step 2: Drop target table in RDS
        rds_cursor.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE;")
        rds_conn.commit()
        print(f"🧨 Dropped table `{target_table}` in RDS.")

        # Step 3: Get schema from Neon
        neon_cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (source_table,))
        schema = neon_cursor.fetchall()

        if not schema:
            print("❌ No schema found in Neon.")
            return {"status": "fail", "error": "empty_source_schema"}

        # Step 4: Create table in RDS
        column_definitions = ", ".join([f"{col} {dtype}" for col, dtype in schema])
        create_table_sql = f"CREATE TABLE {target_table} ({column_definitions});"
        rds_cursor.execute(create_table_sql)
        rds_conn.commit()
        print(f"✅ Created `{target_table}` in RDS.")

        # Step 5: Copy data from Neon
        neon_cursor.execute(f"SELECT * FROM {source_table};")
        rows = neon_cursor.fetchall()

        if not rows:
            print("📭 No data found in source table.")
            return {"status": "success", "message": "schema refreshed, no data"}

        placeholders = ", ".join(["%s"] * len(schema))
        insert_query = f"INSERT INTO {target_table} VALUES ({placeholders});"
        rds_cursor.executemany(insert_query, rows)
        rds_conn.commit()
        print(f"📦 Inserted {len(rows)} rows into `{target_table}` in RDS.")

        return {"status": "success", "rows_copied": len(rows)}

    except Exception as e:
        print(f"❌ Error during refresh: {str(e)}")
        return {"status": "fail", "error": str(e)}

    finally:
        try:
            neon_cursor.close()
            neon_conn.close()
            rds_cursor.close()
            rds_conn.close()
            print("🔒 Closed all DB connections.")
        except Exception as e:
            print(f"⚠️ Error closing connections: {str(e)}")

def refresh_table_from_source(source_table, target_table, neon_cursor, rds_cursor, rds_conn):
    print(f"🔄 Refreshing table `{target_table}` in RDS using `{source_table}` from Neon...")

    # Drop target table in RDS if it exists
    rds_cursor.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE;")
    rds_conn.commit()
    print(f"🧨 Dropped `{target_table}` in RDS.")

    # Get schema from Neon
    neon_cursor.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position;
    """, (source_table,))
    columns = neon_cursor.fetchall()

    if not columns:
        print("⚠️ No columns found in source table.")
        return "error: empty source table schema"

    # Create table in RDS with same schema
    column_definitions = ", ".join([f"{col} {dtype}" for col, dtype in columns])
    create_sql = f"CREATE TABLE {target_table} ({column_definitions});"
    rds_cursor.execute(create_sql)
    rds_conn.commit()
    print(f"✅ Created `{target_table}` in RDS with source schema.")

    # Fetch data from Neon
    neon_cursor.execute(f"SELECT * FROM {source_table};")
    rows = neon_cursor.fetchall()
    if not rows:
        print("📭 No data to copy from source.")
        return "success: schema refreshed, no data"

    # Prepare insert statement
    placeholders = ", ".join(["%s"] * len(columns))
    insert_query = f"INSERT INTO {target_table} VALUES ({placeholders})"
    rds_cursor.executemany(insert_query, rows)
    rds_conn.commit()
    print(f"📦 Inserted {len(rows)} rows into `{target_table}` in RDS.")

    return "success"

def apply_schema_fix(source_schema, target_schema, target_table, rds_cursor):
    print("🔧 Applying schema fix...")
    queries = []
    source_cols = {col: dtype for col, dtype in source_schema}
    target_cols = {col: dtype for col, dtype in target_schema}

    print("📦 Source schema:", source_cols)
    print("🎯 Target schema:", target_cols)

    for src_col, src_dtype in source_cols.items():
        src_dtype_norm = normalize_type(src_dtype)
        tgt_dtype_norm = normalize_type(target_cols.get(src_col, ""))

        if src_col not in target_cols:
            queries.append(f"ALTER TABLE {target_table} ADD COLUMN {src_col} {src_dtype};")
        elif tgt_dtype_norm != src_dtype_norm:
            queries.append(
                f"ALTER TABLE {target_table} ALTER COLUMN {src_col} TYPE {src_dtype} USING {src_col}::{src_dtype};"
            )

    details = "\n".join(queries)
    for query in queries:
        print(f"📤 Running: {query}")
        try:
            rds_cursor.execute(query)
        except Exception as e:
            print(f"❌ Failed to execute query: {query}")
            print("Error:", e)

    print("✅ Schema fix applied.")
    return details

def normalize_type(dtype):
    return dtype.lower().replace('character varying', 'varchar').replace('int4', 'integer')

def call_bedrock(input_json):
    print("🤖 Calling Amazon Bedrock for strategy recommendation...")
    prompt = f"""
    You are a smart data remediation engine.

    Given this input table comparison result:

    {json.dumps(input_json, indent=2)}

    Available remediation strategies are:
    1. fix_values - Fix only mismatched values
    2. refresh_columns - Refresh only mismatched columns
    3. refresh_rows - Refresh only mismatched rows
    4. load_missing - Load missing rows between source and target
    5. full_refresh - Reload full table from source

    Which strategy is the most efficient based on the mismatch metrics, and why? Return only the strategy name in lowercase like:
    {{
      "best_strategy": "fix_values"
    }}
    """
    try:
        
        native_request = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 100000,
            "temperature": 0.1,
            "messages": [
                {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
                }
            ],
        }

        # Convert the native request to JSON.
        request = json.dumps(native_request)

        # Invoke the model with the request.
        response = client.invoke_model(modelId=model_id, body=request)
        model_response = json.loads(response["body"].read())
        response_body = model_response["content"][0]["text"]

        response_text = model_response["content"][0]["text"]
        response_body = json.loads(response_text)  # Convert string to dict
        print(f"✅ Bedrock recommended strategy: {response_body.get('best_strategy')}")
        return response_body.get("best_strategy")

    except Exception as e:
        print(f"⚠️ Bedrock failed: {e}")
        return None

def fallback_rule_based_strategy(input_json):
    print("📊 Using fallback rule-based strategy...")
    metrics = input_json["issue_details"]["metrics"]
    total_rows = metrics["row_count"]
    total_columns = metrics["column_count"]
    value_mismatches = metrics["total_value_mismatches"]
    row_mismatches = metrics["total_row_mismatches"]
    column_mismatches = metrics["total_column_mismatches"]

    strategies = {
        "fix_values": value_mismatches,
        "refresh_columns": row_mismatches * column_mismatches,
        "refresh_rows": row_mismatches * total_columns,
        "load_missing": row_mismatches * total_columns,
        "full_refresh": total_rows * total_columns
    }

    best = min(strategies, key=strategies.get)
    print(f"✅ Fallback selected: {best}")
    return best

def choose_best_strategy(input_json):
    strategy = call_bedrock(input_json)
    valid = {"fix_values", "refresh_columns", "refresh_rows", "load_missing", "full_refresh"}
    if strategy in valid:
        return {"strategy_selected": strategy, "source": "bedrock"}
    else:
        fallback = fallback_rule_based_strategy(input_json)
        return {"strategy_selected": fallback, "source": "fallback"}

def generate_fix_queries_with_bedrock(input_json, primary_key ,target_table , strategy, primary_key_type):
    print("✏️ Generating SQL fix queries using Bedrock...")
    prompt = f"""
    You are an expert SQL agent.

    Given the following mismatch details and selected strategy, generate SQL queries that should be run on Amazon RDS (PostgreSQL compatible) to apply the fix.

    Use this format:
    [
      {{
        "query": "<SQL_QUERY_STRING>",
        "description": "What this query does"
      }}
    ]

    📊 Mismatch JSON:
    {json.dumps(input_json, indent=2)}

    🔧 Selected strategy: "{strategy}"

    The table name is the value of {target_table} and primary keys are {primary_key}".
    The primary key '{primary_key}' is of type {primary_key_type}.
    Make sure to use correct quoting based on the type.

    Only include valid SQL statements for RDS. Do not include explanations outside the JSON.
    Give Only JSON, Do not include starting ```json and trailing ```.
    """

    try:
        
        native_request = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 100000,
            "temperature": 0.1,
            "messages": [
                {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
                }
            ],
        }

        # Convert the native request to JSON.
        request = json.dumps(native_request)

        # Invoke the model with the request.
        response = client.invoke_model(modelId=model_id, body=request)
        model_response = json.loads(response["body"].read())      
        queries_text = model_response["content"][0]["text"]
        print(queries_text)
        queries = json.loads(queries_text)  # Convert to Python list
        print(f"✅ Generated {len(queries)} SQL fix queries.")
        return queries
    except Exception as e:
        print(f"❌ Bedrock query generation failed: {e}")
        return [{"query": "--", "description": "Bedrock failed", "error": str(e)}]

def get_column_type(schema, column_name):
    for col, dtype in schema:
        if col == column_name:
            return dtype.lower()
    return None

def run_fix_queries_on_rds(queries, db_config):
    print("🚀 Executing fix queries on Amazon RDS...")
    results = []
    try:
        conn = psycopg2.connect(**db_config)
        conn.autocommit = True
        cur = conn.cursor()

        for q in queries:
            try:
                print(f"▶️ Running query: {q['query']}")
                cur.execute(q["query"])
                results.append({"query": q["query"], "status": "success"})
            except Exception as e:
                print(f"❌ Query failed: {str(e)}")
                results.append({"query": q["query"], "status": "failed", "error": str(e)})

        cur.close()
        conn.close()
        print("✅ Query execution completed.")
    except Exception as e:
        print(f"❌ Connection to RDS failed: {str(e)}")
        results.append({"error": "connection_failed", "details": str(e)})

    return results



@app.route('/agent/validation', methods=['POST'])
def validate():
    secrets=get_secrets()
    model_id = secrets["BEDROCK_MODEL_ID"]
    region_name = secrets["AWS_REGION"]
    client = boto3.client("bedrock-runtime", region_name=region_name)
    AWS_REGION = secrets["AWS_REGION"]
    BUCKET_NAME = secrets["AWS_S3_BUCKET_NAME"]
    BEDROCK_MODEL_ID = secrets["BEDROCK_MODEL_ID"]
    SENDER_MAIL = secrets["SENDER_MAIL"]
    FLASK_SERVER = secrets["FLASK_SERVER"]
    LOG_AGENT_URL = secrets["LOG_AGENT_API"]
    log_api = secrets["LOG_AGENT_API"]
    notify_api = secrets["NOTIFY_AGENT_API"]
    LOG_AGENT_API = secrets["LOG_AGENT_API"]
    NOTIFY_AGENT_API = secrets["NOTIFY_AGENT_API"]
    NOTIFY_AGENT_URL = secrets["NOTIFY_AGENT_API"]
    print("⚙️ Validator Agent triggered...")
    start_time = datetime.utcnow()
    try:
        event = request.json
        run_id = event["run_id"]
        source_table = event["source_table"]
        target_table = event["target_table"]
        primary_keys = event["primary_key"]

        # Load secrets
        secrets = get_secrets()
        print("🔐 Secrets retrieved successfully")
        rds_config = {
            'host': secrets["RDS_DB_HOST"],
            'port': int(secrets["RDS_DB_PORT"]),
            'dbname': secrets["RDS_DB_NAME"],
            'user': secrets["RDS_DB_USER"],
            'password': secrets["RDS_DB_PASSWORD"]
        }
        neon_conn_str = secrets["NEON_DB_URL"]
        notify_api = secrets["NOTIFY_AGENT_API"]
        log_api = secrets["LOG_AGENT_API"]

        # Stage 1: Schema Check
        print("🚦 Stage 1: Schema Validation")
        success, result = compare_table_schemas(neon_conn_str, rds_config, source_table, target_table)
        if not success:
            return handle_issue("schema", run_id, source_table, target_table, result, start_time, notify_api, log_api)
        # ✅ Row count matched
        print("✅ Stage 1 passed. Proceed to Stage 2.")

        # Stage 2: Row Count Check
        print("🚦 Stage 2: Row Count Validation")
        success, result = compare_row_counts(neon_conn_str, rds_config, source_table, target_table, primary_keys)
        if not success:
            return handle_issue("row-count", run_id, source_table, target_table, result, start_time, notify_api, log_api)
        # ✅ Row count matched
        print("✅ Stage 2 passed. Proceed to Stage 3.")

        # Stage 3: Row Data Check
        comparison_result = compare_row_data(neon_conn_str, rds_config, source_table, primary_keys)
        end_time = datetime.utcnow()
        duration = int((end_time - start_time).total_seconds())

        if not comparison_result["matched"]:
            issue_details = {
                "mismatches": comparison_result["mismatched_rows"],
                "metrics": comparison_result["metrics"]
            }
            notify_body = {
                "type": "issue",
                "timestamp": end_time.isoformat() + "Z",
                "agent": "validation-agent",
                "source_table": source_table,
                "target_table": target_table,
                "details": json.dumps(issue_details, indent=2)
            }
            log_body = {
                "run_id": run_id,
                "log_add": {
                    "agent": "validation-agent",
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "status": "success",
                    "result": "failed",
                    "details": json.dumps(issue_details, indent=2)
                }
            }

            notify=requests.post(notify_api, json=notify_body)
            log=requests.post(log_api, json=log_body)
            print("notfy",notify,log)

            return jsonify({
                "duration": duration,
                "next_agent": "remediation-agent",
                "issue": "row-data",
                "issue_details": issue_details
            })

        # ✅ All validations passed
        print("🎉 Row data validation successful. Tables are fully matched.")
        log_body = {
            "run_id": run_id,
            "log_add": {
                "agent": "validation-agent",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "status": "success",
                "result": "success",
                "details": "Tables are matching, no mismatch found"
            }
        }
        requests.post(log_api, json=log_body)

        return jsonify({
            "duration": int(duration),
            "next_agent": "quality-rule-agent"
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/agent/remediation', methods=['POST'])
def remediate():
    start_time = datetime.utcnow().isoformat()
    start = time.time()

    body = request.get_json()
    run_id = body.get("run_id")
    source_table = body.get("source_table")
    target_table = body.get("target_table")
    primary_key = body.get("primary_key")
    issue_type = body.get("issue_type")
    issue_details = body.get("issue_details")

    rds_config = {
        'host': secrets["RDS_DB_HOST"],
        'port': int(secrets["RDS_DB_PORT"]),
        'dbname': secrets["RDS_DB_NAME"],
        'user': secrets["RDS_DB_USER"],
        'password': secrets["RDS_DB_PASSWORD"]
    }
    neon_conn_str = secrets["NEON_DB_URL"]

    try:
        neon_conn = psycopg2.connect(neon_conn_str)
        neon_cursor = neon_conn.cursor()
        rds_conn = psycopg2.connect(**rds_config)
        rds_cursor = rds_conn.cursor()

        source_schema = get_table_schema(neon_cursor, source_table)
        target_schema = get_table_schema(rds_cursor, target_table)

        # 1. Schema Mismatch
        if issue_type == "schema":
            print("🔍 Resolving schema mismatch...")
            fix_details = apply_schema_fix(source_schema, target_schema, target_table, rds_cursor)
            rds_conn.commit()
            result = "success"
        # 1. Schema Mismatch
        # if issue_type == "schema":
        #     result = refresh_table_from_source(
        #         source_table=source_table,
        #         target_table=target_table,
        #         neon_cursor=neon_cursor,
        #         rds_cursor=rds_cursor,
        #         rds_conn=rds_conn
        #     )
        #     print(result)        
        # 2. Row Count Mismatch
        elif issue_type == "row-count":
            print("🔍 Resolving row count mismatch...")
            missing_rows = []
            for line in issue_details.split('\n'):
                if line.strip().startswith("🔸 Missing PK:"):
                    try:
                        row_data = line.split("→ Row: ")[-1].strip()
                        row_dict = ast.literal_eval(row_data)
                        missing_rows.append(row_dict)
                    except Exception as e:
                        print(f"⚠️ Failed to parse row: {line} — {str(e)}")

            inserted_rows = []
            for row in missing_rows:
                cols = ', '.join(row.keys())
                placeholders = ', '.join(['%s'] * len(row))
                values = list(row.values())
                insert_query = f"INSERT INTO {target_table} ({cols}) VALUES ({placeholders});"
                try:
                    rds_cursor.execute(insert_query, values)
                    inserted_rows.append(row)
                except Exception as e:
                    print(f"❌ Failed to insert row: {row} — {str(e)}")

            rds_conn.commit()
            fix_details = f"✅ Inserted {len(inserted_rows)} missing rows."
            result = "success" if inserted_rows else "fail"

        # 3. Row Data Mismatch
        elif issue_type == "row-data":
            print("🔍 Resolving row data mismatch...")
            strategy_result = choose_best_strategy(body)
            strategy = strategy_result["strategy_selected"]
            print(f"📌 Selected strategy: {strategy} (via {strategy_result['source']})")
            primary_key_type = get_column_type(target_schema, primary_key)
            queries = generate_fix_queries_with_bedrock(body,target_table,primary_key, strategy, primary_key_type)
            execution_results = run_fix_queries_on_rds(queries, rds_config)
            fix_details = json.dumps(execution_results, indent=2)
            refresh_rds_with_neondb(
                source_table="test4",
                target_table="test4",
                neon_conn_str=secrets["NEON_DB_URL"],
                rds_config=rds_config
            )

            result = "success" if all(r.get("status") == "success" for r in execution_results) else "fail"

        else:
            fix_details = f"Issue type '{issue_type}' not supported."
            result = "fail"

        status = "success"

    except Exception as e:
        fix_details = f"❌ Exception occurred: {str(e)}"
        result = "fail"
        status = "fail"
        print(fix_details)

    finally:
        try:
            neon_cursor.close()
            neon_conn.close()
            rds_cursor.close()
            rds_conn.close()
            print("🔒 Closed all DB connections.")
        except:
            pass

    end_time = datetime.utcnow().isoformat()
    duration = int(time.time() - start)

    # Notify
    notify_payload = {
        "type": "fixed",
        "timestamp": end_time + "Z",
        "agent": "remediation-agent",
        "source_table": source_table,
        "target_table": target_table,
        "details": fix_details
    }
    try:
        print("📢 Sending notification to Notify Agent...")
        requests.post(secrets["NOTIFY_AGENT_API"], json=notify_payload)
    except Exception as e:
        print("❌ Notify Agent error:", str(e))

    # Log
    log_payload = {
        "run_id": run_id,
        "log_add": {
            "agent": "remediation-agent",
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "result": result,
            "details": fix_details
        }
    }
    try:
        print("📝 Sending logs to Log Agent...")
        requests.post(secrets["LOG_AGENT_API"], json=log_payload)
    except Exception as e:
        print("❌ Log Agent error:", str(e))

    print("✅ Remediation Agent complete.")
    return {
        "statusCode": 200,
        "duration": duration,
        "next_agent": "validation-agent"
    }

@app.route('/agent/quality', methods=['POST'])
def run_rules():
    start_time = datetime.utcnow()
    start = time.time()
    try:
        body = request.json
        print("📩 Incoming request body:", body)

        source_table = body.get("source-table")
        target_table = body.get("target_table")
        run_id = body.get("run_id")

        print(f"📌 Target table: {target_table}, Source table: {source_table}, Run ID: {run_id}")

        rule_file_key = f"{RULES_PATH}{target_table}_rules.yaml"
        result_file_key = f"{RESULTS_PATH}{target_table}_rules_result.jsonl"

        # Fetch rule file
        try:
            print(f"📥 Downloading rule file from S3: {rule_file_key}")
            rule_obj = s3.get_object(Bucket=BUCKET_NAME, Key=rule_file_key)
            rule_file = yaml.safe_load(rule_obj['Body'].read())
            table_rules = rule_file.get(target_table, {})
            total_rules = len(table_rules.get("table-level", [])) + len(table_rules.get("column-level", []))
            print(f"🧮 Total rules loaded: {total_rules}")

            print("📜 Loaded rules:", json.dumps(rule_file, indent=2))
        except s3.exceptions.NoSuchKey:
            print(f"❌ Rules file not found: {rule_file_key}")
            return {"status": "error", "reason": f"Rules file not found: {rule_file_key}"}

        # Flatten rules
        rules = []
        for scope in ['table-level', 'column-level', 'row-level']:
            scope_rules = table_rules.get(scope, [])
            for idx, rule in enumerate(scope_rules):
                rules.append((f"{scope}-{idx+1}", rule))


        print(f"🧮 Total rules loaded: {len(rules)}")

        # # Normalize 'column' to 'key'
        # for _, rule in rules:
        #     if 'column' in rule:
        #         rule['key'] = rule['column']
        # ✅ Only set 'key' from 'column' if 'key' is missing
        for _, rule in rules:
            if 'key' not in rule and 'column' in rule:
                rule['key'] = rule['column']


        # Connect to RDS
        print("🔌 Connecting to RDS...")
        connection = connect_to_rds()
        cursor = connection.cursor()
        print("✅ RDS connection established.")

        results = []
        fail_count = 0

        for rule_no, rule in rules:
            print(f"🔍 Processing Rule {rule_no}: {rule}")
            query = generate_query_from_bedrock(rule, target_table)
            print(f"🧾 Generated Query:\n{query}")
            if not query:
                print("⚠️ Skipping rule - no query generated.")
                continue

            try:
                cursor.execute(query)
                result_data = cursor.fetchone()
                result = result_data[0] if result_data else None
                print(f"📊 Query Result: {result}")
            except Exception as e:
                print(f"❌ Query execution failed for rule {rule_no}: {e}")
                result = None

            passed = evaluate_rule_result(rule, result)
            status = "pass" if passed else "fail"
            if status == "fail":
                fail_count += 1

            result_entry = {
                "Rule no": rule_no,
                "Rule": rule,
                "Query": query,
                "Result": str(result),
                "Final status": status,
                "Time stamp": datetime.utcnow().isoformat()
            }
            print(f"✅ Rule Evaluation Result: {result_entry}")
            results.append(result_entry)

        # Read existing results
        try:
            existing_obj = s3.get_object(Bucket=BUCKET_NAME, Key=result_file_key)
            existing_data = existing_obj['Body'].read().decode().splitlines()
            print("📦 Loaded existing results from S3.")
        except s3.exceptions.NoSuchKey:
            existing_data = []
            print("ℹ️ No existing result file found, will create a new one.")

        final_result_data = existing_data + [json.dumps(res) for res in results]
        s3.put_object(Bucket=BUCKET_NAME, Key=result_file_key, Body='\n'.join(final_result_data).encode())
        print(f"📤 Results written to: {result_file_key}")

        end_time = datetime.utcnow()
        failing_rules = [f"Rule {res['Rule no']}" for res in results if res['Final status'] == 'fail']
        status_text = "all rules given are passed" if fail_count == 0 else f"Rules which are failing: {', '.join(failing_rules)}"
   
        log_payload = {
            "run_id": run_id,
            "log_add": {
                "agent": "quality-rule-agent",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "status": "success",
                "result": "passed",
                "details": status_text
            }
        }
        print("🪵 Sending log to LOG_AGENT_URL...",log_payload)
        log_response = requests.post(LOG_AGENT_URL, json=log_payload)
        print("🧾 Log response:", log_response.status_code)
        end_time = datetime.utcnow().isoformat()
        duration = int(time.time() - start)

        return jsonify({
            "status": "success",
            "fail_count": fail_count,
            "rules_checked": len(rules),
            "details": results,
            "run_id": run_id,
            "duration": duration
        })

    except Exception as e:
        print("🚨 Exception occurred:", e)
        return jsonify({"status": "error", "reason": str(e)})


# ------------------------ Run Flask App ------------------------

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
