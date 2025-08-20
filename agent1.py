import os
import re
import json
import pyodbc
import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------- CONFIG ----------------
# SQL_SERVER   = os.getenv("SQL_SERVER")
# SQL_DATABASE = os.getenv("SQL_DATABASE")
# SQL_AUTH     = os.getenv("SQL_AUTH", "windows").lower()
# SQL_UID      = os.getenv("SQL_UID", "")
# SQL_PWD      = os.getenv("SQL_PWD", "")
# SQL_DRIVER   = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

# OLLAMA_URL   = os.getenv("OLLAMA_URL", "http://localhost:11434")
# OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral")

# SQL_SERVER=DESKTOP-910H9HI\SQLEXPRESS
# SQL_DATABASE=SalesPlanDB
# SQL_AUTH=windows            # use "sql" if you want SQL Login
# # SQL_UID=                    # fill only if SQL_AUTH=sql
# # SQL_PWD=                    # fill only if SQL_AUTH=sql
# SQL_DRIVER=ODBC Driver 17 for SQL Server

# # --- Ollama ---
# OLLAMA_URL=http://192.168.1.7:11434
# OLLAMA_MODEL=llama3:8b
SQL_SERVER   = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_AUTH     = os.getenv("SQL_AUTH", "windows").lower()
SQL_UID      = os.getenv("SQL_UID", "")
SQL_PWD      = os.getenv("SQL_PWD", "")
SQL_DRIVER   = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

OLLAMA_URL   = os.getenv("OLLAMA_URL", "http://192.168.1.7:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3:8b")
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
    lines.append("")
    lines.append("-- Note: OrderFY contains fiscal year as VARCHAR, e.g., '2023', '2024'")
    return "\n".join(lines)

# ---------------- OLLAMA (NL -> SQL) ----------------
SYSTEM_PROMPT = """You are a senior SQL analyst for Microsoft SQL Server.
Return ONLY a valid T-SQL SELECT statement based on the user's question and the provided schema.

Rules:
- Read-only: do not modify data (no INSERT/UPDATE/DELETE/ALTER/DROP/TRUNCATE/CREATE).
- Use fully qualified names (schema.table).
- Include TOP 100 if result might be large.
- Use only real column names from the schema. Do not invent or modify names.
- NEVER wrap function calls (like YEAR(col), SUM(...)) in square brackets.
- Only wrap actual column or table names in [ ] if they contain spaces or are reserved keywords.
- If a column is VARCHAR but contains year values (e.g., '2023'), use CAST(column AS INT), not YEAR().
- Do not use YEAR(), MONTH(), etc. on non-date columns.
- Do not output markdown unless in a code block.
- Do not add explanations. Only output the SQL query.
- If limiting results, use SELECT TOP 100, not TOP at the end.
- TOP must appear right after SELECT, before the column list.
"""

def generate_sql(question: str, schema_text: str) -> str:
    """Call Ollama to generate SQL from natural language."""
    prompt = f"""{SYSTEM_PROMPT}

SCHEMA:
{schema_text}

QUESTION:
{question}

SQL:"""
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False
    }
    try:
        r = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=500)
        r.raise_for_status()
        raw = r.json().get("response", "").strip()

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

# ---------------- SQL REWRITE: Fix Common LLM Errors ----------------
def rewrite_sql(sql: str) -> str:
    """Fix known LLM-generated errors."""
    # Replace YEAR(col) with CAST(col AS INT) for known year-like columns
    # Only if col name suggests it's a year field (FY, Year, etc.)
    sql = re.sub(
        r"\bYEAR\s*\(\s*(\b\w*F?Y\w*\b)\s*\)",
        r"CAST(\1 AS INT)",
        sql,
        flags=re.IGNORECASE
    )
    # Remove any stray backticks
    sql = sql.replace("`", "")
    return sql.strip()

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

    # Skip leading WITH
    if cleaned.startswith("with "):
        cleaned = cleaned[5:].strip()

    # Must start with SELECT
    if not cleaned.startswith("select"):
        return False

    # Must not contain write keywords
    return not any(kw in cleaned for kw in WRITE_KEYWORDS)

# ---------------- EXECUTION ----------------
def execute_sql(conn, sql: str):
    cur = conn.cursor()
    cur.execute(sql)
    columns = [d[0] for d in cur.description]
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

    print("[*] Reading schema…")
    try:
        schema_text = fetch_schema_text(conn)
    except Exception as e:
        print(f"[ERROR] Schema fetch failed: {e}")
        return
    print("[*] Schema ready.")

    while True:
        try:
            q = input("\nAsk about your data (or 'exit'): ").strip()
            if q.lower() in ("exit", "quit"):
                break
            if not q:
                continue

            # Generate SQL
            sql = generate_sql(q, schema_text)
            sql = rewrite_sql(sql)  # Fix common errors
            print("\n--- Generated SQL ---\n", sql)

            # Safety check
            if not is_safe_sql(sql):
                print("\n[!] Refusing to run non-SELECT or unsafe SQL. Modify your question.")
                continue

            # Execute
            cols, rows = execute_sql(conn, sql)
            print("\n--- Results ---")
            print("\t".join(cols))
            for row in rows:
                print("\t".join("" if v is None else str(v) for v in row))
            print("---------------")

        except requests.RequestException as e:
            print(f"[ERROR] LLM request failed: {e}")
        except pyodbc.Error as e:
            print(f"[ERROR] SQL execution failed: {e}")
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")

    conn.close()
    print("[*] Bye.")

if __name__ == "__main__":
    main()