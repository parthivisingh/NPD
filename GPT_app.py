# GPT_app.py
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
    server = os.getenv("SQL_SERVER", "localhost")
    database = os.getenv("SQL_DATABASE")
    auth = os.getenv("SQL_AUTH", "windows").lower()

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

# Initialize session state
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


# ---------------- Sidebar (Query History - Clean List) ----------------
st.sidebar.header("Query History")

def load_query_from_history():
    selected_q = st.session_state.selected_query
    st.session_state.user_question = selected_q
    st.session_state.results = None
    st.session_state.debug_info = None
    st.session_state.chart_type = None

if st.session_state["history"]:
    # Display as clean selectable dropdown or text list
    st.sidebar.write("Click to reload a past query:")

    # Show history as a vertical list of text buttons
    for i, q in enumerate(reversed(st.session_state["history"])):
        if st.sidebar.button(f"{q}", key=f"hist_btn_{i}"):
            st.session_state.user_question = q
            st.session_state.results = None
            st.session_state.debug_info = None
            st.session_state.chart_type = None
else:
    st.sidebar.caption("No queries yet.")


# ---------------- Main Page ----------------
st.title("Ask Your Database")

# Text input synchronized with session state
user_question = st.text_area(
    "Enter your natural language question:",
    key="user_question"  # Streamlit auto-syncs with st.session_state["user_question"]
)
# Ask Button
ask_btn = st.button(
    "Ask",
    type="primary" if user_question.strip() else "secondary"
)

# # ---------------- Result Renderer ----------------
# def render_result(df: pd.DataFrame, chart_type: str):
#     """Render charts and tables based on query result."""
#     if df is None or df.empty:
#         st.warning("⚠️ No data returned from the query.")
#         return
    
#      # Always show table
#     #st.dataframe(df, use_container_width=True, height=400)
    
#     num_cols = df.shape[1]
#     num_rows = df.shape[0]

#     if num_cols == 1:
#         #st.write(f"**{num_rows} value(s) retrieved:**")
#         # Convert single column to list and display as plain text, one per line
#         col_name = df.columns[0]
#         num_rows = len(df)
#         st.subheader(col_name)
#         values = df.iloc[:, 0].dropna().astype(str).tolist()
#         for val in values:
#             st.text(val)  # Simple, unformatted text

#     elif num_cols <= 10:
#         st.write(f"Showing {num_rows} row(s) with {num_cols} columns.")
#         st.dataframe(df, use_container_width=True, height=400)

#     else:  # More than 10 columns
#         st.write(f"Showing wide result with **{num_cols} columns** and {num_rows} rows (scroll horizontally):")
#         st.dataframe(df, use_container_width=True, height=400)
    
#     # Optional Chart
#     if chart_type and df.shape[1] >= 2:
#         try:
#             x_col, y_col = df.columns[0], df.columns[1]

#             if chart_type == "bar":
#                 chart = alt.Chart(df).mark_bar().encode(
#                     x=alt.X(x_col, sort='-y'),
#                     y=y_col,
#                     tooltip=list(df.columns)
#                 )
#                 st.altair_chart(chart, use_container_width=True)

#             elif chart_type == "stacked_bar" and df.shape[1] >= 3:
#                 color_col = df.columns[2]
#                 chart = alt.Chart(df).mark_bar().encode(
#                     x=x_col,
#                     y=y_col,
#                     color=color_col,
#                     tooltip=list(df.columns)
#                 )
#                 st.altair_chart(chart, use_container_width=True)

#             elif chart_type == "line":
#                 chart = alt.Chart(df).mark_line(point=True).encode(
#                     x=x_col,
#                     y=y_col,
#                     tooltip=list(df.columns)
#                 )
#                 st.altair_chart(chart, use_container_width=True)

#         except Exception as e:
#             st.error(f"📊 Chart rendering failed: {e}")

def render_result(df: pd.DataFrame, chart_type: str):
    """Render charts and data output dynamically based on column count."""
    if df is None or df.empty:
        st.warning("⚠️ No data returned from the query.")
        return


    num_cols = df.shape[1]
    num_rows = df.shape[0]

    if num_cols == 1:
        col_name = df.columns[0]
        num_rows = len(df)

        st.subheader(col_name)

        if num_rows > 10:
            st.dataframe(df, use_container_width=True, height=400)
        else:
            # Keep it simple: plain text, one per line
            values = df.iloc[:, 0].dropna().astype(str).tolist()
            for val in values:
                st.text(val)

    elif num_cols <= 10:
        st.write(f"Showing {num_rows} row(s) with {num_cols} columns.")
        st.dataframe(df, use_container_width=True, height=400)

    else:  # More than 10 columns
        st.write(f"Showing wide result with **{num_cols} columns** and {num_rows} rows (scroll horizontally):")
        st.dataframe(df, use_container_width=True, height=400)
    
    # Show chart if applicable
    if chart_type and len(df) > 0:
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
                x_col, color_col, y_col = df.columns[0], df.columns[1], df.columns[2]
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
            st.error(f"📊 Chart rendering failed: {e}")


# ---------------- Handle Query Execution ----------------
if ask_btn:
    q = st.session_state["user_question"].strip()
    if not q:
        st.warning("❗ Please enter a question before clicking 'Ask'.")
    else:
        # Add to history if new
        if q not in st.session_state["history"]:
            st.session_state["history"].append(q)

        try:
            with pyodbc.connect(build_conn_str()) as conn:
                sql_query, debug_info = process_question(q, conn)

                # Default to empty DataFrame
                df = pd.DataFrame()

                if debug_info.get("result") is not None:
                    if isinstance(debug_info["result"], pd.DataFrame):
                        df = debug_info["result"]
                    elif isinstance(debug_info["result"], list):
                        df = pd.DataFrame(debug_info["result"])
                elif sql_query:
                    df = pd.read_sql(sql_query, conn)
                else:
                    st.info("No SQL query was generated, and no direct result provided.")

                # Save results
                st.session_state["results"] = df
                st.session_state["debug_info"] = debug_info
                st.session_state["chart_type"] = debug_info.get("chart_type")

        except Exception as e:
            st.error(f"❌ Query execution failed: {e}")
            st.session_state["results"] = pd.DataFrame()
            st.session_state["debug_info"] = {"error": str(e), "query": q}
            st.session_state["chart_type"] = None


# ---------------- Display Results ----------------
if st.session_state["results"] is not None:
    #st.header("Results")
    render_result(st.session_state["results"], st.session_state["chart_type"])

    with st.expander("🛠 Debug Output", expanded=False):
        st.json(st.session_state["debug_info"])

    final_sql = (
        st.session_state["debug_info"].get("final_sql")
        if st.session_state["debug_info"]
        else None
    )
    if final_sql:
        st.subheader("🔧 Generated SQL")
        st.code(final_sql, language="sql")