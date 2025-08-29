# streamlit_app.py
import os
import pandas as pd
import streamlit as st
import pyodbc
import urllib
from dotenv import load_dotenv
from sqlalchemy import create_engine
from GPT_agent2 import process_question
import altair as alt

# Load environment variables
load_dotenv()

# ---------------- DB Connection ----------------
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


# ---------------- UI Setup ----------------
st.set_page_config(page_title="Ask Your Database", layout="wide")

# Session state
if "history" not in st.session_state:
    st.session_state["history"] = []
if "user_question" not in st.session_state:
    st.session_state["user_question"] = ""
if "results" not in st.session_state:
    st.session_state["results"] = None
if "debug_info" not in st.session_state:
    st.session_state["debug_info"] = None
if "chart_type" not in st.session_state:
    st.session_state["chart_type"] = None

# ---------------- Sidebar (Query History) ----------------
st.sidebar.header("Query History")

if st.session_state["history"]:
    for i, q in enumerate(reversed(st.session_state["history"])):
        if st.sidebar.button(q, key=f"hist_{i}"):
            # Load query into text box and clear results
            st.session_state["user_question"] = q
            st.session_state["results"] = None
            st.session_state["debug_info"] = None
            st.session_state["chart_type"] = None
            st.rerun()
else:
    st.sidebar.caption("No queries yet.")

# ---------------- Main Page ----------------
st.title("Ask your database")

# Input field
user_question = st.text_area(
    "Enter your query:", 
    value=st.session_state["user_question"], 
    key="user_question_input"
)

# Ask button
ask_btn = st.button(
    "Ask", 
    type="primary" if st.session_state["user_question"] else "secondary"
)

# ---------------- Handle Query Execution ----------------
def render_result(df: pd.DataFrame, chart_type: str):
    """Render charts and tables based on query result."""
    if df is None or df.empty:
        st.warning("⚠️ No results to display.")
        return

    # Optional Graph Output
    if chart_type and df.shape[1] >= 2:
        try:
            if chart_type == "bar":
                x_col, y_col = df.columns[0], df.columns[1]
                chart = alt.Chart(df).mark_bar().encode(
                    x=alt.X(x_col, sort='-y'),
                    y=y_col,
                    tooltip=list(df.columns)
                )
                st.altair_chart(chart, use_container_width=True)

            elif chart_type == "stacked_bar" and df.shape[1] >= 3:
                x_col, color_col, y_col = df.columns[:3]
                chart = alt.Chart(df).mark_bar().encode(
                    x=x_col,
                    y=y_col,
                    color=color_col,
                    tooltip=list(df.columns)
                )
                st.altair_chart(chart, use_container_width=True)

            elif chart_type == "line":
                x_col, y_col = df.columns[0], df.columns[1]
                chart = alt.Chart(df).mark_line(point=True).encode(
                    x=x_col,
                    y=y_col,
                    tooltip=list(df.columns)
                )
                st.altair_chart(chart, use_container_width=True)

        except Exception as e:
            st.error(f"Chart rendering failed: {e}")

    # Always show Table Output
    st.subheader("Tabular Output")
    st.dataframe(df, use_container_width=True, height=400)


if ask_btn:
    if not user_question.strip():
        st.warning("Please enter a question first.")
    else:
        try:
            conn = pyodbc.connect(build_conn_str())
            sql_query, debug_info = process_question(user_question, conn)

            # Save query to history
            if user_question not in st.session_state["history"]:
                st.session_state["history"].append(user_question)

            # Store results in session
            result = debug_info.get("result")
            df = None
            if isinstance(result, pd.DataFrame):
                df = result
            elif isinstance(result, list) and len(result) > 0:
                df = pd.DataFrame(result)

            st.session_state["results"] = df
            st.session_state["debug_info"] = debug_info
            st.session_state["chart_type"] = debug_info.get("chart_type")

        except Exception as e:
            st.error(f"Error: {e}")

# ---------------- Display Results ----------------
if st.session_state["results"] is not None:
    st.header("Results")
    render_result(st.session_state["results"], st.session_state["chart_type"])

    with st.expander("🛠 Debug Output", expanded=False):
        st.json(st.session_state["debug_info"])

    final_sql = (
        st.session_state["debug_info"].get("final_sql")
        if st.session_state["debug_info"]
        else None
    )
    if final_sql:
        st.subheader("Generated SQL")
        st.code(final_sql, language="sql")
