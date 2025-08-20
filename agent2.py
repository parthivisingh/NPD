import os
import re
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

OLLAMA_URL   = os.getenv("OLLAMA_URL", "http://192.168.1.7:11434")
# OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3:8b")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral:7b-instruct")

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
SYSTEM_PROMPT = """You are a senior SQL analyst for Microsoft SQL Server.
Return ONLY a valid T-SQL SELECT statement based on the user's question and the provided schema.

Rules:
- Read-only: do not modify data (no INSERT/UPDATE/DELETE/ALTER/DROP/TRUNCATE/CREATE).
- Use fully qualified names (schema.table).
- Include TOP 100 by default if result might be large.
- Use ONLY the exact column names from the schema. NEVER invent or modify column names.
- NEVER use a column that is not listed in the schema.
- Example: If schema shows [OrderFY], do not use [Ord_FY], [FY], or [OrderYear].
- Do not wrap function calls (like SUM(...), CAST(...)) in square brackets.
- Only wrap actual column or table names in [ ] if they contain spaces or are reserved keywords.
- If a column is VARCHAR but contains year values (e.g., '2023'), use CAST(column AS INT), not YEAR().
- Do not use YEAR(), MONTH(), etc. on non-date columns.
- Place TOP 100 immediately after SELECT: SELECT TOP 100 ...
- Do not put TOP at the end of the query.
- Do not output markdown unless in a code block.
- Do not add explanations. Only output the SQL query.
- If OrderFY contains values like '2024-25', extract the first year using LEFT(OrderFY, 4), then CAST to INT.
- Do not use CAST(OrderFY AS INT) directly — it will fail.
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

# ---------------- SQL REWRITE & CORRECTION ----------------
def rewrite_sql(sql: str) -> str:
    # Fix 1: Ensure TOP 100 is in correct place
    sql = re.sub(r"\s+TOP\s+100", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"^SELECT\b", "SELECT TOP 100", sql, flags=re.IGNORECASE)

    # Fix 2: Replace any CAST(OrderFY AS INT) or SUBSTRING with consistent LEFT
    # Normalize all to LEFT(OrderFY, 4) for simplicity
    if "OrderFY" in sql:
        # Replace various forms with standard LEFT
        sql = re.sub(
            r"CAST\s*\(\s*SUBSTRING\s*\(\s*OrderFY\s*,\s*1\s*,\s*4\s*\)\s*AS\s+INT\s*\)",
            r"CAST(LEFT(OrderFY, 4) AS INT)",
            sql,
            flags=re.IGNORECASE
        )
        sql = re.sub(
            r"CAST\s*\(\s*OrderFY\s+AS\s+INT\s*\)",
            r"CAST(LEFT(OrderFY, 4) AS INT)",
            sql,
            flags=re.IGNORECASE
        )

    # Fix 3: Add GROUP BY if aggregation is used and GROUP BY is missing
    has_aggregation = bool(re.search(r"\bSUM\(|\bCOUNT\(|\bAVG\(|\bMIN\(|\bMAX\(", sql, re.IGNORECASE))
    has_group_by = "GROUP BY" in sql.upper()

    if has_aggregation and not has_group_by:
        # Look for the grouped expression in SELECT
        match = re.search(r"CAST\(LEFT\(OrderFY,\s*4\)\s+AS\s+INT\)", sql, re.IGNORECASE)
        if match:
            expr = "CAST(LEFT(OrderFY, 4) AS INT)"
            if "ORDER BY" in sql.upper():
                # Insert before ORDER BY
                sql = re.sub(
                    r"\s+ORDER BY",
                    f"\nGROUP BY {expr}\nORDER BY",
                    sql,
                    flags=re.IGNORECASE
                )
            else:
                sql += f"\nGROUP BY {expr}"

    # Fix 4: Add ORDER BY if not present (optional, improves UX)
    if has_aggregation and "ORDER BY" not in sql.upper():
        if "FY" in sql:
            sql += "\nORDER BY FY"
        elif "CAST(LEFT(OrderFY, 4) AS INT)" in sql:
            sql += "\nORDER BY CAST(LEFT(OrderFY, 4) AS INT)"

    # Fix 5: Clean up double brackets
    sql = re.sub(r"\[\[([^\]]+)\]\]", r"[\1]", sql)  # [[Amount]] → [Amount]
    sql = re.sub(r"\[\[([^\]]+)\]", r"[\1]", sql)
    sql = re.sub(r"\[([^\]]+)\]\]", r"[\1]", sql)

    return sql.strip()

def correct_columns(sql: str, fuzzy_map: dict) -> str:
    """Correct common misspelled column names using fuzzy mapping."""
    # Find unbracketed or bracketed column-like tokens
    tokens = re.finditer(r"\b[\[\]a-zA-Z0-9_]+\b", sql)
    for match in reversed(list(tokens)):
        token = match.group(0)
        # Skip SQL keywords
        if token.upper() in {
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "TOP", "AS",
            "SUM", "COUNT", "AVG", "MIN", "MAX", "CAST", "INT", "INTO", "EXEC"
        }:
            continue
        # Clean token for matching
        clean = token.strip("[]").replace("_", "").replace(" ", "").lower()
        if clean in fuzzy_map:
            replacement = f"[{fuzzy_map[clean]}]"
            # Replace only this instance
            start = match.start()
            end = match.end()
            sql = sql[:start] + replacement + sql[end:]
    return sql

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

    print("[*] Reading schema and building column map…")
    try:
        schema_text = fetch_schema_text(conn)
        column_fuzzy_map = get_column_mapping(conn)
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

            # Step 1: Generate SQL
            sql = generate_sql(q, schema_text)
            print("\n--- Raw Generated SQL ---")
            print(sql)

            # Step 2: Rewrite for known issues
            sql = rewrite_sql(sql)

            # Step 3: Correct column names (e.g., Ord_FY → OrderFY)
            sql = correct_columns(sql, column_fuzzy_map)
            print("\n--- Final Corrected SQL ---")
            print(sql)

            # Step 4: Safety check
            if not is_safe_sql(sql):
                print("\n[!] Refusing to run non-SELECT or unsafe SQL.")
                continue

            # Step 5: Execute
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