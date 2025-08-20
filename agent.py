import os
import re
import json
import pyodbc
import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------- CONFIG ----------------
SQL_SERVER   = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_AUTH     = os.getenv("SQL_AUTH", "windows").lower()
SQL_UID      = os.getenv("SQL_UID", "")
SQL_PWD      = os.getenv("SQL_PWD", "")
SQL_DRIVER   = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")

OLLAMA_URL   = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral")

# ---------------- DB CONNECTION ----------------
def build_conn_str() -> str:
    """
    Build a DSN-less ODBC connection string for Windows.
    Driver 17 
    """
    base = [
            f"DRIVER={{{SQL_DRIVER}}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={SQL_UID};"
            f"PWD={SQL_PWD};"
            "TrustServerCertificate=yes;"
    ]
    if SQL_AUTH == "sql":
        base += [f"UID={SQL_UID}", f"PWD={SQL_PWD}"]
    else:
        base += ["Trusted_Connection=yes"]
    return ";".join(base)

def get_connection():
    conn_str = build_conn_str()
    try:
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        raise RuntimeError(f"DB connect failed: {e}\nConnStr={conn_str}")

# ---------------- SCHEMA INTROSPECTION ----------------
def fetch_schema_text(conn, include_schemas=("dbo",), limit_tables=50) -> str:
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

    # --- ADD THIS: HINTS FOR LLM ---
    lines.append("")
    lines.append("-- HINTS:")
    lines.append("-- OrderFY: VARCHAR(10), contains fiscal year as '2023', '2024'. Use CAST(OrderFY AS INT) to group by year.")
    lines.append("-- Amount: monetary value")
    lines.append("-- Avoid using monthyear unless filtering by month. For yearly totals, use OrderFY.")

    return "\n".join(lines)

# ---------------- OLLAMA (NL -> SQL) ----------------
# SYSTEM_PROMPT = """You are a senior SQL analyst for Microsoft SQL Server.
# Return ONLY a valid T-SQL SELECT statement based on the user's question and the provided schema.

# Rules:
# - Read-only: do not modify data (no INSERT/UPDATE/DELETE/ALTER/DROP/TRUNCATE/CREATE).
# - Prefer fully qualified names like schema.table and bracket identifiers with spaces.
# - If a result could be large, include TOP 100 by default.
# - Use only real column names from the schema provided. Do not invent or modify column names.
# - NEVER wrap function expressions like YEAR(column) in square brackets. Only wrap actual column or table names with spaces/special characters.
# - If a column is VARCHAR but contains year values (e.g., '2023'), CAST it to INT: CAST(column AS INT), not YEAR(column).
# - Do not use YEAR(), MONTH(), etc. on non-date columns.
# - If unsure, select the raw column instead.
# - Do not add explanations, only output the SQL query.
# - Always wrap table and column names in square brackets [ ] ONLY if they contain spaces or special characters.
# - NEVER wrap SQL keywords or function names in square brackets.
# - Only use square brackets around actual column or table names that contain spaces or reserved keywords.
# - Example: [Order Date] is correct. [YEAR(OrderDate)] is invalid.
# - Do not output markdown if not in a code block.
# - If the user asks for "FY", "fiscal year", or "by year", use the OrderFY column.
# - OrderFY contains values like '2023', '2024' — cast to INT: CAST(OrderFY AS INT)
# - Do not invent WHERE clauses unless explicitly requested.
# - Never assume data ranges — skip WHERE unless needed.
# - Avoid using monthyear for yearly aggregations.
# """
SYSTEM_PROMPT = """You are a senior SQL analyst for Microsoft SQL Server.
Return ONLY a valid T-SQL SELECT statement based on the user's question and the provided schema.

Rules:
- Read-only: no INSERT/UPDATE/DELETE/ALTER/DROP/TRUNCATE/CREATE.
- Use schema.table format.
- For "by FY", "by year", or "fiscal year": USE COLUMN [OrderFY].
- OrderFY is VARCHAR(10) with values like '2023', '2024' — use CAST(OrderFY AS INT) to convert.
- NEVER use monthyear for yearly totals unless explicitly asked.
- Do not add WHERE clauses unless user asks for filters.
- Use TOP 100 if result might be large.
- Do not invent column names or logic.
- Do not use YEAR(), MONTH() on non-date columns.
- Do not wrap function calls in [].
- Only wrap actual column/table names with spaces in [].
- Output only SQL, no explanations.
- Example: 
    Input: Total amount by FY
    Output: SELECT TOP 100 CAST(OrderFY AS INT) AS FiscalYear, SUM(Amount) AS TotalAmount FROM dbo.SalesPlanTable GROUP BY CAST(OrderFY AS INT) ORDER BY FiscalYear
"""

def generate_sql(question: str, schema_text: str) -> str:
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
    r = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=500)
    r.raise_for_status()
    sql = r.json().get("response", "").strip()
    # Strip code fences if present
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql

# ---------------- SAFETY ----------------
WRITE_KEYWORDS = ("insert", "update", "delete", "alter", "drop", "truncate", "create", "merge", "exec")

def is_safe_sql(sql: str) -> bool:
    # crude but effective guard
    s = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.S).lower().strip()
    return s.startswith("select") and not any(k in s for k in WRITE_KEYWORDS)

# ---------------- EXECUTION ----------------
def execute_sql(conn, sql: str):
    cur = conn.cursor()
    cur.execute(sql)
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return columns, rows

# ---------------- FORMATTING ----------------
SQL_KEYWORDS = {
    "select","from","where","group","by","order","top","distinct",
    "sum","count","avg","min","max","join","on","and","or","as",
    "having","union","all","desc","asc"
}

def fix_identifiers(sql: str) -> str:
    """
    Only wrap column or table names with spaces in [ ].
    Do NOT touch SQL keywords or function calls.
    """
    # Split SQL into tokens, but preserve structure
    # Simple: find unquoted, unbracketed words with spaces, that aren't SQL functions
    tokens = re.finditer(r'\b(\w+\s+\w+)\b', sql)
    for match in tokens:
        word = match.group(1)
        # Avoid wrapping if it's part of a function: YEAR(col), SUM(...), etc.
        if re.search(rf"\b{re.escape(word)}\s*\(", sql):
            continue  # Likely a function or bad context
        # Avoid if already in brackets
        if re.search(rf"\[{re.escape(word)}\]", sql):
            continue
        # Replace only standalone instances
        sql = sql.replace(word, f"[{word}]")
    return sql

def rewrite_sql(sql: str) -> str:
    """
    Fix common LLM-generated SQL errors.
    """
    # Replace YEAR(some_column) where column is VARCHAR with CAST(some_column AS INT)
    # Only if the column name contains "FY", "Year", or similar
    def replace_year_call(match):
        col = match.group(1)
        return f"CAST({col} AS INT)"

    # Detect: YEAR(OrderFY), YEAR( [OrderFY] ), etc.
    sql = re.sub(
        r"\bYEAR\s*\(\s*(?:\[)?(\w+)(?:\])?\s*\)",
        replace_year_call,
        sql,
        flags=re.IGNORECASE
    )

    # Optional: ensure GROUP BY uses same expression as SELECT alias
    # This is advanced; can be skipped initially

    return sql


def rewrite_sql(sql: str) -> str:
    """
    Fix common LLM-generated SQL errors.
    """
    # Remove unsafe or invalid BETWEEN clauses on year columns
    sql = re.sub(
        r"\s+WHERE\s+[\w\[\]]+\s+BETWEEN\s+'[\d-]+' AND '[\d-]+'",
        "",
        sql,
        flags=re.IGNORECASE
    )

    # Replace YEAR(col) with CAST(col AS INT) if col looks like a year field
    sql = re.sub(
        r"\bYEAR\s*\(\s*(?:\[)?(\w*F?Y\w*)\s*(?:\])?\s*\)",
        r"CAST(\1 AS INT)",
        sql,
        flags=re.IGNORECASE
    )

    # Ensure CAST is used, not direct comparison
    return sql.strip()


# ---------------- MAIN LOOP ----------------
def main():
    print("[*] Connecting to SQL Server…")
    conn = get_connection()
    print("[*] Connected.")

    print("[*] Reading schema…")
    schema_text = fetch_schema_text(conn)
    print("[*] Schema ready.")

    while True:
        q = input("\nAsk about your data (or 'exit'): ").strip()
        if q.lower() in ("exit", "quit"):
            break

        try:
            sql = generate_sql(q, schema_text)
            sql = fix_identifiers(sql)
            sql = rewrite_sql(sql)
            print("\n--- Generated SQL ---\n", sql)

            if not is_safe_sql(sql):
                print("\n[!] Refusing to run non-SELECT or unsafe SQL. Modify your question.")
                continue

            cols, rows = execute_sql(conn, sql)
            print("\n--- Results ---")
            print("\t".join(cols))
            for r in rows:
                print("\t".join("" if v is None else str(v) for v in r))
            print("---------------")
        except Exception as e:
            print(f"[ERROR] {e}")

    conn.close()
    print("[*] Bye.")

if __name__ == "__main__":
    main()
