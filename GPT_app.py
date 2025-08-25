import os
import pandas as pd
import streamlit as st
import pyodbc
import urllib
from dotenv import load_dotenv
from sqlalchemy import create_engine
from GPT_agent2 import process_question

# Load environment
load_dotenv()

def build_conn_str() -> str:
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
    else:
        return (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
            "TrustServerCertificate=yes"
        )

def get_engine():
    conn_str = build_conn_str()
    params = urllib.parse.quote_plus(conn_str)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# --- UI ---
st.set_page_config(page_title="QueryDB", layout="wide")
st.title("QueryDB - Natural Language to SQL")

user_question = st.text_area("Ask a question about your database:")

if st.button("Run Query"):
    if not user_question.strip():
        st.warning("Please enter a question first.")
    else:
        try:
            conn = pyodbc.connect(build_conn_str())

            sql_query, debug_info = process_question(user_question, conn)

            if not sql_query:
                st.error("No SQL was generated.")
                with st.expander("🛠 Debug Info"):
                    st.json(debug_info)
            else:
                st.subheader("Generated SQL")
                st.code(sql_query, language="sql")

                # Check if process_question already ran query
                if "result" in debug_info and debug_info["result"] is not None:
                    st.subheader("Results")
                    st.dataframe(debug_info["result"])
                else:
                    # Otherwise, run SQL here
                    df = pd.read_sql(sql_query, engine)
                    st.subheader("📊 Results")
                    st.dataframe(df)

                with st.expander("🛠 Debug Info"):
                    st.json(debug_info)

        except Exception as e:
            st.error(f"Error: {e}")
