# agent2.py

import os
import re
import pyodbc
import requests
from dotenv import load_dotenv
import traceback
import json

from intent_router import generate_sql as generate_sql_template
from intent_router import detect_intent
from intent_router import SYNONYM_MAP
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
        # Scrub password
        redacted = re.sub(r"PWD=[^;]+", "PWD=***", conn_str)
        raise RuntimeError(f"DB connect failed: {e}\nConnStr={redacted}")

# ---------------- SCHEMA INTROSPECTION ----------------
def fetch_schema_text(conn, include_schemas=("dbo",), limit_tables=50) -> str:
    cur = conn.cursor()
    placeholders = ",".join("?" for _ in include_schemas)
    cur.execute(f"""
        SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA IN ({placeholders}) AND TABLE_NAME = 'SalesPlanTable'
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
    lines.append("-- Note: [monthyear] = 'Apr-24', 'May-25' — use for month-year filtering")
    return "\n".join(lines)

# ---------------- DYNAMIC SYNONYM FILTERING ----------------
def extract_relevant_synonyms(question: str, full_map: dict) -> dict:
    """
    Extract only the synonyms that appear in the question.
    Reduces LLM context noise.
    """
    q = question.lower().strip()
    relevant = {"columns": {}}

    # Extract relevant columns
    for col, synonyms in full_map.get("columns", {}).items():
        if any(syn.lower().strip() in q for syn in synonyms):
            relevant["columns"][col] = synonyms

    # Optionally: add intent/metrics if needed
    # But usually not needed — intent is already passed separately
    return relevant

# ---------------- LLM SQL GENERATION ----------------
def generate_sql_with_context(question: str, schema_text: str, intent: str, full_synonym_map: dict) -> str:
    """
    Generate SQL using LLM with **only relevant synonyms**.
    """
    # Extract only what's mentioned
    relevant_map = extract_relevant_synonyms(question, full_synonym_map)
    available_columns = list(relevant_map["columns"].keys())
    column_synonyms = relevant_map["columns"]

    prompt = f"""
You are a precise SQL assistant for Microsoft SQL Server. Generate ONLY a SELECT query.

## Rules
- Return ONLY the SQL query. No explanations.
- Use SELECT to answer the question.
- Use ONLY column names from the schema. Do NOT invent or modify column names.
- Wrap column names in [ ] if they have spaces or are keywords.
- For "previous month", use [monthyear] = 'Jul-25' (replace with actual value).
- Do NOT use 'Previous Month' as a string value.
- Do NOT use INSERT, UPDATE, DELETE, or DDL.
- Do NOT output markdown or code fences.

## Context
Intent: {intent}
Relevant Columns: {available_columns}
Column Synonyms: {column_synonyms}
Schema:
{schema_text}

## Question
{question}

SQL:
""".strip()

    headers = {"Content-Type": "application/json"}
    if LLM_API_KEY:
        headers["Authorization"] = f"Bearer {LLM_API_KEY}"

    try:
        r = requests.post(
            f"{LLM_URL}/chat/completions",
            json={
                "model": LLM_MODEL,
                "messages": [
                    {"role": "system", "content": "You are a helpful SQL assistant."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.0,
                "max_tokens": 500,
                "stream": False
            },
            headers=headers,
            timeout=30
        )
        r.raise_for_status()
        raw = r.json()["choices"][0]["message"]["content"].strip()
        return extract_sql_from_response(raw)
    except Exception as e:
        raise RuntimeError(f"LLM call failed: {e}")

def extract_sql_from_response(text: str) -> str:
    """
    Extract SQL from LLM response (with or without markdown).
    """
    if "```sql" in text:
        match = re.search(r"```sql\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
        return match.group(1).strip() if match else text
    elif "```" in text:
        match = re.search(r"```\s*(.*?)\s*```", text, re.DOTALL)
        return match.group(1).strip() if match else text
    return text.strip().replace("`", "")

# ---------------- SAFETY CHECK ----------------
def is_safe_sql(sql: str) -> bool:
    """Check if SQL is safe (read-only SELECT)."""
    if not sql:
        return False
    cleaned = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.MULTILINE | re.DOTALL)
    cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()

    if cleaned.startswith("with "):
        cleaned = cleaned[5:].strip()

    if not cleaned.lstrip(" (").startswith("select"):
        return False

    WRITE_KEYWORDS = ("insert", "update", "delete", "alter", "drop", "truncate", "create", "merge", "exec", "into")
    return not any(kw in cleaned for kw in WRITE_KEYWORDS)

# ---------------- EXECUTION ----------------
def execute_sql(conn, sql: str):
    cur = conn.cursor()
    cur.execute(sql)
    if cur.description is None:
        return [], []
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
                print("No template matched. Using LLM with context...")

                # Get intent and relevant synonyms
                intent = detect_intent(q)

                # Call LLM with **only relevant synonyms**
                raw_sql = generate_sql_with_context(q, schema_text, intent, SYNONYM_MAP)
                print("\n--- Raw LLM Output ---")
                print(raw_sql)

                # Repair SQL (fix [MMMYY] → [MMMMYY], etc.)
                sql = guard.repair_sql(raw_sql)
            else:
                print("\n--- Using Template-Based SQL ---")
                print(sql)

            print("\n--- Final SQL ---")
            print(sql)
            
            # intent = detect_intent(q)
            # if intent == "compare" and re.search(r"\bWHERE.*?\bmonthyear\b", sql, re.I):
            #     print("\n[!] Blocked [monthyear] filter in 'compare' query.")
            #     continue

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
            if cols and rows:
                print("\t".join(cols))
                for row in rows:
                    print("\t".join("" if v is None else str(v) for v in row))
            else:
                print("(No rows returned)")
            print("---------------")

        except requests.exceptions.RequestException as e:
            print(f"[ERROR] LLM API request failed: {e}")
        except pyodbc.Error as e:
            print(f"[ERROR] SQL execution failed: {e}")
        except Exception as e:
            print(f"[ERROR] Unexpected error: {e}")
            traceback.print_exc()

    conn.close()
    print("[*] Bye.")

if __name__ == "__main__":
    main()