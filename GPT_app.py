# Streamlit app
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

import streamlit as st
import pandas as pd
import altair as alt  # or use st.bar_chart / st.line_chart for quick plots

def render_chart(df: pd.DataFrame, chart_type: str):
    if chart_type == "bar":
        # assume first col categorical, second numeric for now
        x_col, y_col = df.columns[0], df.columns[1]
        chart = alt.Chart(df).mark_bar().encode(
            x=alt.X(x_col, sort='-y'),
            y=y_col,
            tooltip=list(df.columns)
        )
        st.altair_chart(chart, use_container_width=True)

    elif chart_type == "stacked_bar":
        # assume first = category, second = sub-category, third = numeric
        x_col, color_col, y_col = df.columns[:3]
        chart = alt.Chart(df).mark_bar().encode(
            x=x_col,
            y=y_col,
            color=color_col,
            tooltip=list(df.columns)
        )
        st.altair_chart(chart, use_container_width=True)

    elif chart_type == "line":
        # for time-series intent
        x_col, y_col = df.columns[0], df.columns[1]
        chart = alt.Chart(df).mark_line(point=True).encode(
            x=x_col,
            y=y_col,
            tooltip=list(df.columns)
        )
        st.altair_chart(chart, use_container_width=True)

    else:
        st.write("No suitable chart type suggested.")
        
def render_result(df: pd.DataFrame, chart_type: str):
    if df is None or df.empty:
        st.warning("⚠️ No results to display.")
        return

    # Case 1: Single cell result (1 row, 1 col) → show as metric
    if df.shape == (1, 1):
        colname = df.columns[0]
        value = df.iloc[0, 0]
        st.metric(label=colname, value=value)
        return

    # Case 2: One or more columns → always show table (scrollable)
    st.subheader("Query Results")
    st.dataframe(df, use_container_width=True)

    # Optional: also show visualization if chart_type is valid
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


if st.button("Run Query"):
    if not user_question.strip():
        st.warning("Please enter a question first.")
    else:
        try:
            conn = pyodbc.connect(build_conn_str())
            sql_query, debug_info = process_question(user_question, conn)

            # Ensure we handle different result formats
            result = debug_info.get("result")
            df = None
            if isinstance(result, pd.DataFrame):
                df = result
            elif isinstance(result, list) and len(result) > 0:
                df = pd.DataFrame(result)

            # Render chart or table
            chart_type = debug_info.get("chart_type")
            if df is not None:
                render_result(df, chart_type)
            else:
                st.warning("⚠️ No results returned from query.")
                
            # Always show debug info
            with st.expander("🛠 Debug Info", expanded=True):
                st.json(debug_info)

            # Show generated SQL
            final_sql = debug_info.get("final_sql") or sql_query
            if final_sql:
                st.subheader("Generated SQL")
                st.code(final_sql, language="sql")
            else:
                st.error("❌ No SQL was generated.")

        except Exception as e:
            st.error(f"Error: {e}")

