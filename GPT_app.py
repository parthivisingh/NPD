import streamlit as st
import pyodbc
import pandas as pd
from GPT_agent2 import process_question  # <-- wrap your main pipeline into a function
import os
import pyodbc
from dotenv import load_dotenv


load_dotenv()

import os
import pyodbc
from dotenv import load_dotenv

# Load .env only once
load_dotenv()

def build_conn_str() -> str:
    """Build SQL Server connection string from environment variables."""
    server   = os.getenv("SQL_SERVER", "localhost")
    database = os.getenv("SQL_DATABASE")
    auth     = os.getenv("SQL_AUTH", "windows").lower()

    if auth == "sql":
        uid = os.getenv("SQL_UID")
        pwd = os.getenv("SQL_PWD")
        return (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};DATABASE={database};UID={uid};PWD={pwd};"
            "TrustServerCertificate=yes"
        )
    else:  # Windows auth
        return (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
            "TrustServerCertificate=yes"
        )

def get_connection():
    """Return a live DB connection using env vars."""
    conn_str = build_conn_str()
    print(f"[*] Connecting with: {conn_str}")  # DEBUG — remove in prod
    return pyodbc.connect(conn_str, timeout=5)


st.set_page_config(page_title="QueryDB PoC", layout="wide")
st.title("💬 QueryDB - Natural Language to SQL")

# Input
user_question = st.text_area("Ask a question about your database:")

if st.button("Run Query"):
    if not user_question.strip():
        st.warning("Please enter a question first.")
    else:
        try:
            conn = get_connection()

            # Process NL → SQL (wrap your existing logic)
            sql_query, debug_info = process_question(user_question, conn)

            st.subheader("🔎 Generated SQL")
            st.code(sql_query, language="sql")

            # Run the query
            df = pd.read_sql(sql_query, conn)
            st.subheader("📊 Results")
            st.dataframe(df)

            # Optional debug info
            with st.expander("🛠 Debug Info"):
                st.json(debug_info)

        except Exception as e:
            st.error(f"Error: {str(e)}")
