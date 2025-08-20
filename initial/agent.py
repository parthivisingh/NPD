# # agent.py
# import os
# import logging
# import urllib.parse
# from dotenv import load_dotenv

# from sqlalchemy import create_engine

# # LangChain / Ollama / SQL
# from langchain.sql_database import SQLDatabase
# from langchain.agents import create_sql_agent
# from langchain.agents.agent_toolkits import SQLDatabaseToolkit
# from langchain_community.llms import Ollama

# load_dotenv()
# logging.basicConfig(level=logging.INFO)

# # ---------- CONFIG ----------
# DB_SERVER = os.getenv("DB_SERVER")
# DB_NAME = os.getenv("DB_NAME")
# DB_USER = os.getenv("DB_USER")
# DB_PASS = os.getenv("DB_PASS")
# ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
# TRUSTED = os.getenv("TRUSTED_CONNECTION", "false").lower() in ("1", "true", "yes")

# OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "mistral")
# OLLAMA_URL = os.getenv("OLLAMA_URL", None)  # optional

# # ---------- BUILD DB URI ----------
# if TRUSTED:
#     odbc_str = f"DRIVER={{{ODBC_DRIVER}}};SERVER={DB_SERVER};DATABASE={DB_NAME};Trusted_Connection=yes;TrustServerCertificate=yes"
# else:
#     odbc_str = f"DRIVER={{{ODBC_DRIVER}}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASS};TrustServerCertificate=yes"

# params = urllib.parse.quote_plus(odbc_str)
# DB_URI = f"mssql+pyodbc:///?odbc_connect={params}"

# # ---------- CREATE SQLDatabase ----------
# logging.info("Connecting to SQL Server...")
# db = SQLDatabase.from_uri(DB_URI)   # LangChain will introspect schema

# # ---------- CREATE LLM (Ollama) ----------
# llm_kwargs = {"model": OLLAMA_MODEL, "temperature": 0}
# if OLLAMA_URL:
#     # some wrappers accept base_url, some default to localhost; setting if available
#     llm_kwargs["base_url"] = OLLAMA_URL

# llm = Ollama(**llm_kwargs)

# # ---------- CREATE AGENT ----------
# toolkit = SQLDatabaseToolkit(db=db, llm=llm)
# agent_executor = create_sql_agent(llm=llm, toolkit=toolkit, verbose=True)

# # ---------- SAFE WRAPPER (basic) ----------
# FORBIDDEN = ("delete ","drop ","truncate ","alter ","update ","insert ","create ")

# def is_user_query_safe(q: str) -> bool:
#     """Basic check to avoid obviously destructive prompts.
#        Note: the agent will generate SQL itself — implement DB user permissions for real safety."""
#     ql = q.lower()
#     return not any(tok in ql for tok in FORBIDDEN)

# def ask_db(user_question: str) -> str:
#     """Run the LangChain SQL agent and return the answer string."""
#     if not is_user_query_safe(user_question):
#         return "Refusing to run potentially destructive request."
#     try:
#         # agent_executor.run will ask the LLM to produce SQL, execute it and return a natural language answer
#         return agent_executor.run(user_question)
#     except Exception as e:
#         logging.exception("Agent failed:")
#         return f"Error: {e}"

# if __name__ == "__main__":
#     # quick smoke test
#     q = "Show me the top 5 rows from dbo.[Sales Plan Validation]"   # example; adjust to a real table you have
#     print(ask_db(q))
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
    """
    Pull a concise schema summary for the LLM:
    table -> columns (name type)
    """
    cur = conn.cursor()
    # Limit to specific schemas to avoid overwhelming the LLM
    cur.execute("""
        SELECT TOP 1000
            TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA IN ({schemas})
        ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """.format(schemas=",".join("?" for _ in include_schemas)), include_schemas)
    rows = cur.fetchall()

    # group by table
    from collections import defaultdict
    tables = defaultdict(list)
    for sch, tbl, col, dtype in rows:
        tables[(sch, tbl)].append((col, dtype))

    # limit number of tables included (POC)
    items = list(tables.items())[:limit_tables]
    lines = []
    for (sch, tbl), cols in items:
        col_str = ", ".join(f"{c} {t}" for c, t in cols[:80])
        lines.append(f"{sch}.{tbl}({col_str})")
    return "\n".join(lines)

# ---------------- OLLAMA (NL -> SQL) ----------------
SYSTEM_PROMPT = """You are a senior SQL analyst for Microsoft SQL Server.
Return ONLY a valid T-SQL SELECT statement based on the user's question and the provided schema.

Rules:
- Read-only: do not modify data (no INSERT/UPDATE/DELETE/ALTER/DROP/TRUNCATE/CREATE).
- Prefer fully qualified names like schema.table and bracket identifiers with spaces.
- If a result could be large, include TOP 100 by default.
- Use only real column names from the schema provided. Do not invent or modify column names.
- Never call a SQL function (like MONTH, YEAR, SUM, COUNT, etc.) without specifying a valid column inside.
- If unsure about a function, simply select the raw column instead.
- Do not add explanations, only output the SQL query.
- Do not create variations with underscores.
- Always wrap table names and column names in square brackets [ ] if they contain spaces or special characters
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
    r = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=300)
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
def fix_identifiers(sql: str) -> str:
    """
    Ensure identifiers with spaces or special chars are wrapped in [ ].
    Example: Document Type -> [Document Type]
    """
    # Find sequences of words separated by a space that look like identifiers
    # (not keywords like 'order by')
    tokens = re.findall(r'(?<!\[)(\b\w+\s+\w+\b)(?!\])', sql)
    for t in tokens:
        sql = sql.replace(t, f"[{t}]")
    return sql


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
