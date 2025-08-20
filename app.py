# # app.py
# import chainlit as cl
# from agent import ask_db

# @cl.on_message
# async def main(message: str):
#     answer = ask_db(message)
#     await cl.Message(content=answer).send()
import chainlit as cl
from agent import get_connection, fetch_schema_text, generate_sql, is_safe_sql, execute_sql

conn = None
schema_text = None

@cl.on_chat_start
async def start():
    global conn, schema_text
    conn = get_connection()
    schema_text = fetch_schema_text(conn)
    await cl.Message("Connected. Ask me about your data!").send()

@cl.on_message
async def on_message(msg: str):
    try:
        sql = generate_sql(msg, schema_text)
        if not is_safe_sql(sql):
            await cl.Message("Refusing to run non-SELECT/unsafe SQL.").send()
            return
        cols, rows = execute_sql(conn, sql)
        text = " | ".join(cols) + "\n" + "\n".join(" | ".join("" if v is None else str(v) for v in r) for r in rows[:100])
        await cl.Message(f"**SQL:**\n```\n{sql}\n```\n\n**Results (first 100):**\n{txt_truncate(text, 4000)}").send()
    except Exception as e:
        await cl.Message(f"Error: {e}").send()

def txt_truncate(s, n):
    return s if len(s) <= n else s[:n] + "…"
