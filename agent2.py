import os
import re
import pyodbc
import requests
from dotenv import load_dotenv

from intent_router import generate_sql as generate_sql_template
from sql_guard import SQLGuard

load_dotenv()

# ---------------- CONFIG ----------------
SQL_SERVER   = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_AUTH     = os.getenv("SQL_AUTH", "windows").lower()
SQL_UID      = os.getenv("SQL_UID", "")
SQL_PWD      = os.getenv("SQL_PWD", "")
SQL_DRIVER   = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

# --- LLM (Fireworks or Ollama) ---
LLM_URL      = os.getenv("LLM_URL", "https://api.fireworks.ai/inference/v1")
LLM_MODEL    = os.getenv("LLM_MODEL", "accounts/fireworks/models/llama-v3p1-8b-instruct")
LLM_API_KEY  = os.getenv("LLM_API_KEY", "")


# ---------------- DB CONNECTION ----------------
def build_conn_str() -> str:
    """Build a DSN-less ODBC connection string."""
    parts = [
        f"DRIVER={{{SQL_DRIVER}}}",
        f"SERVER={SQL_SERVER}",
        f"DATABASE={SQL_DATABASE}",
        "TrustServerCertificate=yes"
    ]
    if SQL_AUTH == "sql":
        parts += [f"UID={SQL_UID}", f"PWD={SQL_PWD}"]
    else:
        parts += ["Trusted_Connection=yes"]
    return ";".join(parts)

def get_connection():
    conn_str = build_conn_str()
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        raise RuntimeError(f"DB connect failed: {e}\nConnStr={conn_str}")

# ---------------- SCHEMA INTROSPECTION ----------------
def fetch_schema_text(conn, include_schemas=("dbo",), limit_tables=50) -> str:
    """Fetch concise schema: table -> columns (name type)."""
    cur = conn.cursor()
    placeholders = ",".join("?" for _ in include_schemas)
    cur.execute(f"""
        SELECT TOP 1000
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA IN ({placeholders})
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """, include_schemas)
    rows = cur.fetchall()

    from collections import defaultdict
    tables = defaultdict(list)
    for sch, tbl, col, dtype in rows:
        tables[(sch, tbl)].append((col, dtype))

    items = list(tables.items())[:limit_tables]
    lines = []
    for (sch, tbl), cols in items:
        col_str = ", ".join(f"{c} {t}" for c, t in cols[:80])
        lines.append(f"{sch}.{tbl}({col_str})")

    # Add hint for LLM
    lines.append("")
    lines.append("-- Note: OrderFY is VARCHAR(10) containing year like '2023'. Use CAST(OrderFY AS INT) to treat as number.")
    lines.append("-- Note: [MMMMYY] = 'APR25', 'JUL25' — use for month-year filtering")
    lines.append("-- Do NOT use [MMMYY] — it does not exist.")
    return "\n".join(lines)

def get_column_mapping(conn):
    """
    Build a fuzzy map from common misspellings to real column names.
    Example: ord_fy -> OrderFY
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
    """)
    rows = cur.fetchall()
    fuzzy_map = {}

    for sch, tbl, col in rows:
        key = col.lower().replace("_", "").replace(" ", "")
        fuzzy_map[key] = col

        # Also map common abbreviations
        if "fy" in key:
            fuzzy_map[key.replace("fy", "")] = col
        if "year" in key:
            fuzzy_map[key.replace("year", "")] = col

    return fuzzy_map

# ---------------- OLLAMA (NL -> SQL) ----------------
SYSTEM_PROMPT = """You are a helpful assistant that translates natural language into T-SQL SELECT queries for Microsoft SQL Server.

Guidelines:
- Return ONLY the SQL query, no explanations.
- Use SELECT to answer the question.
- Use only column names from the schema. Do not invent new column names or modify existing column names.
- Wrap column names in [ ] only if they have spaces or are reserved keywords.
- Do not use INSERT, UPDATE, DELETE, or DDL commands.
- Do not output markdown or code fences.
"""

def generate_sql(question: str, schema_text: str) -> str:
    """Call Fireworks (or other OpenAI-compatible LLM) to generate SQL from natural language."""
    try:
        headers = {"Content-Type": "application/json"}
        if LLM_API_KEY:
            headers["Authorization"] = f"Bearer {LLM_API_KEY}"

        r = requests.post(
            f"{LLM_URL}/chat/completions",
            json={
                "model": LLM_MODEL,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"SCHEMA:\n{schema_text}\n\nQUESTION:\n{question}\n\nSQL:"}
                ],
                "stream": False
            },
            headers=headers,
            timeout=500
        )
        r.raise_for_status()

        data = r.json()
        # Fireworks is OpenAI-compatible: result is in choices[0].message.content
        raw = data["choices"][0]["message"]["content"].strip()

        # Extract SQL from code blocks
        if "```sql" in raw:
            match = re.search(r"```sql\s*(.*?)\s*```", raw, re.DOTALL | re.IGNORECASE)
            return match.group(1).strip() if match else raw
        elif "```" in raw:
            match = re.search(r"```\s*(.*?)\s*```", raw, re.DOTALL)
            return match.group(1).strip() if match else raw
        return raw.replace("`", "").strip()

    except Exception as e:
        raise RuntimeError(f"Failed to generate SQL: {e}")


# ---------------- SAFETY CHECK ----------------
WRITE_KEYWORDS = ("insert", "update", "delete", "alter", "drop", "truncate", "create", "merge", "exec")

def is_safe_sql(sql: str) -> bool:
    """Check if SQL is safe (read-only SELECT)."""
    if not sql:
        return False
    # Remove comments
    cleaned = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.MULTILINE | re.DOTALL)
    # Normalize whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()

    # Handle WITH CTE
    if cleaned.startswith("with "):
        cleaned = cleaned[5:].strip()

    # Must start with SELECT
    if not cleaned.startswith("select"):
        return False

    # Block write operations
    return not any(kw in cleaned for kw in WRITE_KEYWORDS)

# ---------------- EXECUTION ----------------
def execute_sql(conn, sql: str):
    cur = conn.cursor()
    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    return columns, rows

# ---------------- MAIN LOOP ----------------
def main():
    print("[*] Connecting to SQL Server…")
    try:
        conn = get_connection()
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
        return
    print("[*] Connected.")

    # Initialize SQLGuard
    try:
        guard = SQLGuard(conn)
        print("[*] SQLGuard initialized. Column resolver ready.")
    except Exception as e:
        print(f"[ERROR] Failed to initialize SQLGuard: {e}")
        return

    print("[*] Reading schema…")
    try:
        schema_text = fetch_schema_text(conn)
    except Exception as e:
        print(f"[ERROR] Schema fetch failed: {e}")
        return
    print("[*] Schema ready.")
    #
    while True:
        try:
            q = input("\nAsk about your data (or 'exit'): ").strip()
            if q.lower() in ("exit", "quit"):
                break
            if not q:
                continue

            # Step 1: Try template-based SQL
            sql = generate_sql_template(q, schema_text)

            if sql is None:
                print("No template matched. Using LLM...")
                raw_sql = generate_sql(q, schema_text)
                sql = guard.repair_sql(raw_sql)
                print("\n--- Raw Generated SQL ---")
                print(raw_sql)
            else:
                print("\n--- Using Template-Based SQL ---")
                print(sql)

            print("\n--- Repaired SQL ---")
            print(sql)

            # Step 2: Validate
            if not guard.validate_sql(sql):
                print("\n[!] Invalid SQL logic or column names. Refusing to run.")
                continue

            if not is_safe_sql(sql):
                print("\n[!] Refusing to run unsafe SQL.")
                continue

            # Step 3: Execute
            cols, rows = execute_sql(conn, sql)
            print("\n--- Results ---")
            print("\t".join(cols))
            for row in rows:
                print("\t".join("" if v is None else str(v) for v in row))
            print("---------------")

        except requests.exceptions.RequestException as e:
            print(f"[ERROR] LLM API request failed: {e}")
        except pyodbc.Error as e:
            print(f"[ERROR] SQL execution failed: {e}")
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")

    conn.close()
    print("[*] Bye.")

if __name__ == "__main__":
    main()