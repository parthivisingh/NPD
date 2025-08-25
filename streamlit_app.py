import streamlit as st
import pandas as pd
import traceback

# Import your agent logic
from agent2 import get_connection, fetch_schema_text, SQLGuard, ask_question

# Page config
st.set_page_config(page_title="NLQ SQL Assistant", layout="wide")
st.title("Natural Language to SQL Assistant")

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

# Connect to DB on startup
if "conn" not in st.session_state:
    with st.spinner("Connecting to database..."):
        conn = get_connection()
        if conn:
            st.session_state.conn = conn
            st.session_state.schema_text = fetch_schema_text(conn)
            st.session_state.guard = SQLGuard(conn)
            st.success("Connected to database.")
        else:
            st.error("Failed to connect to database.")
            st.stop()
            
def verify_table_structure(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'SalesPlanTable'
        ORDER BY ORDINAL_POSITION
    """)
    columns = [row[0] for row in cur.fetchall()]
    print(f"[DEBUG] Actual columns in SalesPlanTable: {columns}")
    return columns

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "sql" in message:
            with st.expander("SQL Query", expanded=False):
                st.code(message["sql"], language="sql")
        if "df" in message:
            st.dataframe(message["df"], use_container_width=True)

# User input
if prompt := st.chat_input("Ask about your data..."):
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get response
    with st.spinner("Thinking..."):
        response = ask_question(
            prompt,
            st.session_state.conn,
            st.session_state.schema_text,
            st.session_state.guard
        )

    # Show assistant response
    with st.chat_message("assistant"):
        if response["error"]:
            st.error(f"{response['error']}")
        else:
            st.markdown("Here's the result:")
            with st.expander("SQL Query", expanded=False):
                st.code(response["sql"], language="sql")

            if response["columns"] and response["results"]:
                df = pd.DataFrame(response["results"], columns=response["columns"])
                st.dataframe(df, use_container_width=True)
                # Save for display
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": "Here's the result:",
                    "sql": response["sql"],
                    "df": df
                })
            else:
                st.info("No results returned.")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": "No results returned.",
                    "sql": response["sql"]
                })