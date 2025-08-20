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
- If MonthName contains month names like 'January', 'April', do NOT use CAST(MonthName AS INT).
- To filter for April to June, use: MonthName IN ('April', 'May', 'June').
- Do not assume numeric values unless the schema says so.
- If filtering by numeric month, use columns like Order_Month_Number or document_Month_Number, not MonthName.
- When the user says "compare", return aggregated values (SUM, COUNT) for each group.
- Do not return raw rows unless asked for "list" or "show rows".
- Example: "Compare sales in 2023 vs 2024" → use GROUP BY or PIVOT to show totals per year.
- When asked to "compute growth" between two fiscal years, return:
  - SUM for the first year
  - SUM for the second year
  - Absolute growth (difference)
  - Percentage growth: (new - old)/old * 100
- Use CASE WHEN OrderFY = '2024-25' THEN Amount ... END pattern.
- Do not use window functions like OVER() for simple year-over-year growth.
- Use the exact OrderFY values (e.g., '2024-25'), not CAST to INT.
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

# ---------------- SQL REWRITE & CORRECTION ----------------
def rewrite_sql(sql: str) -> str:
    # --- Fix 1: Handle TOP correctly ---
    top_match = re.search(r"SELECT\s+TOP\s+\d+", sql, re.IGNORECASE)
    if top_match:
        # Keep the existing TOP N
        pass  # Do nothing — preserve original
    else:
        # No TOP found → add TOP 100
        sql = re.sub(r"^SELECT\b", "SELECT TOP 100", sql, flags=re.IGNORECASE)

    # --- Fix 2: Normalize OrderFY handling ---
    if "OrderFY" in sql:
        # Replace any CAST(... AS INT) on OrderFY with LEFT(OrderFY, 4)
        sql = re.sub(
            r"CAST\s*\(\s*[^)]*?OrderFY[^)]*?AS\s+INT\s*\)",
            r"CAST(LEFT(OrderFY, 4) AS INT)",
            sql,
            flags=re.IGNORECASE
        )

    # --- Fix 3: Fix WHERE conditions like "OrderFY >= 2025" ---
    # Since OrderFY is VARCHAR, comparing as INT won't work for '2025-26'
    # We should use LEFT(OrderFY, 4) = '2025' or similar
    if "OrderFY" in sql and "WHERE" in sql.upper():
        # Look for patterns like CAST(...OrderFY...) >= 2025
        where_pattern = r"CAST\s*\(\s*[^)]*?OrderFY[^)]*?AS\s+INT\s*\)\s*(>=|<=|=|>|<)\s*(['\"]?)(20\d{2})\2"
        matches = re.finditer(where_pattern, sql, re.IGNORECASE)
        for match in reversed(list(matches)):
            op = match.group(1)
            year = match.group(3)
            # Replace with LEFT(OrderFY, 4) = '2025'
            replacement = f"LEFT(OrderFY, 4) {op} '{year}'"
            sql = sql[:match.start()] + replacement + sql[match.end():]

    # --- Fix 4: Add GROUP BY if missing ---
    # --- Fix 4: Add GROUP BY if missing and using aggregation ---
    has_aggregation = bool(re.search(r"\bSUM\(|\bCOUNT\(|\bAVG\(|\bMIN\(|\bMAX\(", sql, re.IGNORECASE))
    has_group_by = "GROUP BY" in sql.upper()

    if has_aggregation and not has_group_by:
        # Look for any non-aggregated expressions in SELECT
        # Example: CAST(LEFT(OrderFY, 4) AS INT)
        select_match = re.search(r"SELECT\s+.*?FROM", sql, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_part = select_match.group(0)
            # Find expressions that are not in aggregates
            group_cols = []

            # Extract non-aggregated expressions
            for expr in re.finditer(r"CAST\(LEFT\(OrderFY,\s*4\)\s+AS\s+INT\)", select_part, re.IGNORECASE):
                group_cols.append("CAST(LEFT(OrderFY, 4) AS INT)")
            for expr in re.finditer(r"LEFT\(OrderFY,\s*4\)", select_part, re.IGNORECASE):
                if "CAST" not in expr.group(0):
                    group_cols.append("LEFT(OrderFY, 4)")
            for expr in re.finditer(r"\bOrderFY\b", select_part, re.IGNORECASE):
                group_cols.append("[OrderFY]")

            # Remove duplicates
            group_cols = list(dict.fromkeys(group_cols))

            if group_cols:
                group_by_clause = f"\nGROUP BY {', '.join(group_cols)}"
                if "ORDER BY" in sql.upper():
                    sql = re.sub(r"\s+ORDER BY", group_by_clause + "\nORDER BY", sql, flags=re.IGNORECASE)
                else:
                    sql += group_by_clause

    # --- Fix 5: Clean up double brackets ---
    sql = re.sub(r"\[\[([^\]]+)\]\]", r"[\1]", sql)  # [[Amount]] → [Amount]
    sql = re.sub(r"\[\[([^\]]+)\]", r"[\1]", sql)
    sql = re.sub(r"\[([^\]]+)\]\]", r"[\1]", sql)

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