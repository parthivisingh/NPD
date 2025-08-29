# Streamlit app with two-panel layout
import os
import pandas as pd
import streamlit as st
import pyodbc
import urllib
from dotenv import load_dotenv
from sqlalchemy import create_engine
from GPT_agent2 import process_question
import altair as alt

# Load environment
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
st.set_page_config(page_title="QueryDB", layout="wide")

# Initialize session state for history
if "history" not in st.session_state:
    st.session_state["history"] = []
if "user_question" not in st.session_state:
    st.session_state["user_question"] = ""

# Two panels
left_col, right_col = st.columns([1, 3])

# ---------------- Left Panel ----------------
with left_col:
    st.markdown("<div style='background-color:#f0f0f0; padding:15px; border-radius:8px;'>", unsafe_allow_html=True)
    st.subheader("Ask your database")

    user_question = st.text_area(
        "Query:", 
        value=st.session_state["user_question"], 
        key="user_question_input"
    )

    run_btn = st.button("Run Query")

    st.markdown("### Query History (session only)")
    if st.session_state["history"]:
        for i, q in enumerate(reversed(st.session_state["history"])):
            if st.button(q, key=f"hist_{i}"):
                st.session_state["user_question"] = q
                st.rerun()
    else:
        st.caption("No queries yet.")

    st.markdown("</div>", unsafe_allow_html=True)

# ---------------- Right Panel ----------------
with right_col:
    st.markdown("<div style='background-color:#1c1c1c; color:white; padding:15px; border-radius:8px;'>", unsafe_allow_html=True)
    st.subheader("Results")

    def render_result(df: pd.DataFrame, chart_type: str):
        if df is None or df.empty:
            st.warning("⚠️ No results to display.")
            return

        # Case 1: Single cell → metric
        if df.shape == (1, 1):
            colname = df.columns[0]
            value = df.iloc[0, 0]
            st.metric(label=colname, value=value)
            return

        # Case 2: Show table
        st.dataframe(df, use_container_width=True, height=400)

        # Optional chart
        try:
            if chart_type == "bar" and df.shape[1] >= 2:
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

            elif chart_type == "line" and df.shape[1] >= 2:
                x_col, y_col = df.columns[0], df.columns[1]
                chart = alt.Chart(df).mark_line(point=True).encode(
                    x=x_col,
                    y=y_col,
                    tooltip=list(df.columns)
                )
                st.altair_chart(chart, use_container_width=True)
        except Exception as e:
            st.error(f"Chart rendering failed: {e}")

    # Run Query Handling
    if run_btn:
        if not user_question.strip():
            st.warning("Please enter a question first.")
        else:
            try:
                conn = pyodbc.connect(build_conn_str())
                sql_query, debug_info = process_question(user_question, conn)

                # Save to history
                if user_question not in st.session_state["history"]:
                    st.session_state["history"].append(user_question)
                st.session_state["user_question"] = user_question

                # Handle results
                result = debug_info.get("result")
                df = None
                if isinstance(result, pd.DataFrame):
                    df = result
                elif isinstance(result, list) and len(result) > 0:
                    df = pd.DataFrame(result)

                chart_type = debug_info.get("chart_type")
                if df is not None:
                    render_result(df, chart_type)
                else:
                    st.warning("⚠️ No results returned from query.")

                # Debug info
                with st.expander("🛠 Debug Info", expanded=True):
                    st.json(debug_info)

                # Show SQL
                final_sql = debug_info.get("final_sql") or sql_query
                if final_sql:
                    st.subheader("Generated SQL")
                    st.code(final_sql, language="sql")
                else:
                    st.error("❌ No SQL was generated.")

            except Exception as e:
                st.error(f"Error: {e}")

    st.markdown("</div>", unsafe_allow_html=True)
