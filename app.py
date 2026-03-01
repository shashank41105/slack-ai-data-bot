import os
import io
import csv
import psycopg2
import sqlparse
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler
from flask import Flask, request
from langchain_groq import ChatGroq
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from slack_sdk import WebClient

load_dotenv()

# ── Slack App Setup ──────────────────────────────────────────────────────────
app = App(token=os.environ["SLACK_BOT_TOKEN"])
client = WebClient(token=os.environ["SLACK_BOT_TOKEN"])

# ── Authentication ────────────────────────────────────────────────────────────
ALLOWED_USER_IDS = os.environ.get("ALLOWED_USER_IDS", "").split(",")

# ── LangChain + Groq Setup ───────────────────────────────────────────────────
llm = ChatGroq(
    model="llama-3.3-70b-versatile",
    temperature=0,
    groq_api_key=os.environ["GROQ_API_KEY"]
)

prompt_template = PromptTemplate(
    input_variables=["question"],
    template="""
You are a SQL expert. Convert the user's question into a single PostgreSQL SELECT statement.

You have access to these tables:

Table 1: public.sales_daily
Columns:
  - date       (date)         : the calendar date of sales
  - region     (text)         : sales region, e.g. North, South, East, West
  - category   (text)         : product category, e.g. Electronics, Grocery, Fashion
  - revenue    (numeric 12,2) : total revenue for that day/region/category
  - orders     (integer)      : number of orders placed
  - created_at (timestamptz)  : row creation timestamp

Table 2: public.customers
Columns:
  - customer_id  (serial)       : unique customer ID
  - name         (text)         : full name of customer
  - email        (text)         : email address
  - region       (text)         : customer region
  - signup_date  (date)         : date customer signed up
  - total_orders (integer)      : total number of orders placed
  - total_spent  (numeric 12,2) : total amount spent by customer

Table 3: public.products
Columns:
  - product_id     (serial)       : unique product ID
  - product_name   (text)         : name of the product
  - category       (text)         : product category
  - price          (numeric 10,2) : price of the product
  - stock_quantity (integer)      : current stock available
  - supplier       (text)         : supplier name
  - created_at     (timestamptz)  : row creation timestamp

Rules:
- Output ONLY the SQL query, nothing else.
- A single SELECT statement only.
- No explanations, no markdown, no backticks, no code blocks.
- End the query with a semicolon.

User question: {question}
SQL:"""
)

retry_prompt_template = PromptTemplate(
    input_variables=["question", "bad_sql", "error"],
    template="""
You are a SQL expert. Your previous SQL query failed. Fix it.

Original question: {question}
Your previous SQL: {bad_sql}
Error received: {error}

Generate a corrected single PostgreSQL SELECT statement.
Rules:
- Output ONLY the SQL query, nothing else.
- A single SELECT statement only.
- No explanations, no markdown, no backticks, no code blocks.
- End the query with a semicolon.

Corrected SQL:"""
)

sql_chain = prompt_template | llm | StrOutputParser()
retry_chain = retry_prompt_template | llm | StrOutputParser()

# ── Store last query result ───────────────────────────────────────────────────
last_query_data = {"columns": [], "rows": [], "sql": ""}

# ── Welcome Message Builder ───────────────────────────────────────────────────
def build_welcome_message():
    return {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "👋 *Hello! Welcome to AI Data Bot!*\n\nI can answer questions about your data using natural language — no SQL knowledge needed!\n\n*📊 Available Tables:*\n• `sales_daily` — Daily sales by region and category\n• `customers` — Customer details and spending\n• `products` — Product inventory and pricing"
                }
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*💡 What would you like to explore today?*\nClick a category below or type `/ask-data <your question>`"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "📈 Sales Data"},
                        "action_id": "suggest_sales",
                        "style": "primary"
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "👥 Customers"},
                        "action_id": "suggest_customers"
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "📦 Products"},
                        "action_id": "suggest_products"
                    }
                ]
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*🚀 Quick Start Examples:*"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Revenue by Region"},
                        "action_id": "quick_revenue_region"
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Top Customers"},
                        "action_id": "quick_top_customers"
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Low Stock Products"},
                        "action_id": "quick_low_stock"
                    }
                ]
            }
        ]
    }

# ── Suggestions by Category ───────────────────────────────────────────────────
def build_suggestions_message(category: str):
    suggestions = {
        "sales": {
            "title": "📈 *Sales Data Suggestions*",
            "queries": [
                ("Total revenue by region", "show total revenue by region ordered by highest"),
                ("Revenue by category", "show total revenue by category"),
                ("Daily sales summary", "show total revenue and orders for each date"),
                ("Best performing day", "which date had the highest total revenue"),
                ("Date range revenue", "show revenue between 2025-09-01 and 2025-09-02"),
            ]
        },
        "customers": {
            "title": "👥 *Customer Data Suggestions*",
            "queries": [
                ("Top spenders", "show top 5 customers by total spent"),
                ("Customers by region", "show number of customers by region"),
                ("Recent signups", "show customers ordered by signup date descending"),
                ("High value customers", "show customers with total spent greater than 10000"),
                ("Average spending", "show average total spent by region"),
            ]
        },
        "products": {
            "title": "📦 *Product Data Suggestions*",
            "queries": [
                ("Products by category", "show all products ordered by category"),
                ("Low stock alert", "show products with stock quantity less than 100"),
                ("Most expensive", "show top 5 most expensive products"),
                ("Stock by category", "show total stock quantity by category"),
                ("Supplier list", "show distinct suppliers and their product count"),
            ]
        }
    }

    data = suggestions.get(category, suggestions["sales"])
    query_buttons = []

    for label, _ in data["queries"]:
        query_buttons.append({
            "type": "button",
            "text": {"type": "plain_text", "text": label},
            "action_id": f"run_query_{category}_{label.lower().replace(' ', '_')}"
        })

    query_list = "\n".join(
        [f"• `/ask-data {q}`" for _, q in data["queries"]]
    )

    return {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{data['title']}\n\nHere are some questions you can ask:\n\n{query_list}"
                }
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "Need something else?"}
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "📈 Sales"},
                        "action_id": "suggest_sales"
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "👥 Customers"},
                        "action_id": "suggest_customers"
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "📦 Products"},
                        "action_id": "suggest_products"
                    }
                ]
            }
        ]
    }

# ── SQL Validation ────────────────────────────────────────────────────────────
def validate_sql(sql: str):
    try:
        cleaned = sql.strip().rstrip(";")
        parsed = sqlparse.parse(cleaned)
        if not parsed:
            return False, "Could not parse the generated SQL."
        statement = parsed[0]
        query_type = statement.get_type()
        if query_type != "SELECT":
            return False, f"Only SELECT queries are allowed. Got: {query_type}"
        dangerous = ["drop", "delete", "insert", "update", "truncate",
                     "alter", "create", "grant", "revoke", "exec", "execute"]
        sql_lower = sql.lower()
        for word in dangerous:
            if word in sql_lower:
                return False, f"Blocked: forbidden keyword '{word}'"
        return True, ""
    except Exception as e:
        return False, str(e)

# ── Database Helper ───────────────────────────────────────────────────────────
def run_sql(query: str):
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        cur = conn.cursor()
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchmany(10)
        cur.close()
        return columns, rows
    finally:
        conn.close()

# ── Log Query ─────────────────────────────────────────────────────────────────
def log_query(user_id, user_name, question, sql, status, error=None, rows_count=0):
    try:
        conn = psycopg2.connect(os.environ["DATABASE_URL"])
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.query_logs
            (user_id, user_name, question, generated_sql, status, error_message, rows_returned)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (user_id, user_name, question, sql, status, error, rows_count))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Logging error: {e}")

# ── Format Results ────────────────────────────────────────────────────────────
def format_results(columns: list, rows: list) -> str:
    if not rows:
        return "No results found."
    col_widths = [len(str(col)) for col in columns]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))
    header = " | ".join(str(col).ljust(col_widths[i]) for i, col in enumerate(columns))
    separator = "-+-".join("-" * w for w in col_widths)
    data_rows = [
        " | ".join(str(val).ljust(col_widths[i]) for i, val in enumerate(row))
        for row in rows
    ]
    table = "\n".join([header, separator] + data_rows)
    return f"*Query Result:*\n```\n{table}\n```"

# ── Chart Generator ───────────────────────────────────────────────────────────
def is_date_range_query(sql: str) -> bool:
    sql_lower = sql.lower()
    return "between" in sql_lower or (
        sql_lower.count("date") > 1 and (">" in sql_lower or "<" in sql_lower)
    )

def generate_chart(columns, rows):
    df = pd.DataFrame(rows, columns=columns)
    fig, ax = plt.subplots(figsize=(8, 4))
    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    text_cols = df.select_dtypes(exclude='number').columns.tolist()
    if numeric_cols and text_cols:
        ax.bar(df[text_cols[0]].astype(str), df[numeric_cols[0]], color='steelblue')
        ax.set_xlabel(text_cols[0])
        ax.set_ylabel(numeric_cols[0])
        ax.set_title(f"{numeric_cols[0]} by {text_cols[0]}")
        plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100)
    buf.seek(0)
    plt.close()
    return buf

# ── Core Query Executor ───────────────────────────────────────────────────────
def execute_query(question, user_id, user_name, channel_id, respond):
    sql_query = ""
    try:
        sql_query = sql_chain.invoke({"question": question}).strip()
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

        is_valid, validation_error = validate_sql(sql_query)
        if not is_valid:
            respond(f"⚠️ *Query Blocked:*\n```\n{validation_error}\n```")
            log_query(user_id, user_name, question, sql_query, "BLOCKED", validation_error)
            return

        columns, rows = None, None
        last_error = None

        for attempt in range(3):
            try:
                columns, rows = run_sql(sql_query)
                break
            except Exception as db_error:
                last_error = str(db_error)
                if attempt < 2:
                    sql_query = retry_chain.invoke({
                        "question": question,
                        "bad_sql": sql_query,
                        "error": last_error
                    }).strip()
                    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

        if columns is None:
            respond(f"*Error after retries:*\n```\n{last_error}\n```")
            log_query(user_id, user_name, question, sql_query, "ERROR", last_error)
            return

        last_query_data["columns"] = columns
        last_query_data["rows"] = rows
        last_query_data["sql"] = sql_query

        log_query(user_id, user_name, question, sql_query, "SUCCESS", rows_count=len(rows))
        result_text = format_results(columns, rows)

        respond({
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Generated SQL:*\n```\n{sql_query}\n```\n\n{result_text}"
                    }
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "🔍 *Need something else?*"}
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "📥 Export CSV"},
                            "action_id": "export_csv",
                            "style": "primary"
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "💡 More Suggestions"},
                            "action_id": "show_welcome"
                        }
                    ]
                }
            ]
        })

        if is_date_range_query(sql_query) and rows:
            chart_buf = generate_chart(columns, rows)
            client.files_upload_v2(
                channel=channel_id,
                file=chart_buf,
                filename="chart.png",
                title="📊 Query Chart"
            )

    except Exception as e:
        respond(f"*Error:*\n```\n{str(e)}\n```")
        log_query(user_id, user_name, question, sql_query or "N/A", "ERROR", str(e))

# ── /ask-data Slash Command ───────────────────────────────────────────────────
@app.command("/ask-data")
def handle_ask_data(ack, respond, command):
    ack()

    user_id = command.get("user_id", "")
    user_name = command.get("user_name", "unknown")
    channel_id = command.get("channel_id")
    question = command.get("text", "").strip()

    # Auth check
    if ALLOWED_USER_IDS and user_id not in ALLOWED_USER_IDS:
        respond(f"⛔ Access denied. User ID `{user_id}` is not authorized.")
        return

    # No question = show welcome
    if not question:
        respond(build_welcome_message())
        return

    # Has question = execute
    execute_query(question, user_id, user_name, channel_id, respond)

# ── DM Handler (hi, hello, help) ─────────────────────────────────────────────
@app.event("message")
def handle_message(event, say):
    # Ignore bot messages
    if event.get("bot_id"):
        return

    text = event.get("text", "").lower().strip()
    user_id = event.get("user", "")

    greetings = ["hi", "hello", "hey", "help", "start", "hii", "helo"]

    if any(text == g for g in greetings):
        say(build_welcome_message())
    elif "thank" in text or "thanks" in text:
        say({
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "😊 You're welcome! Let me know if you need anything else.\n\nType `/ask-data <question>` anytime to query your data!"
                    }
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "💡 Show Suggestions"},
                            "action_id": "show_welcome"
                        }
                    ]
                }
            ]
        })
    elif "what can you do" in text or "how" in text:
        say({
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "🤖 *Here's what I can do:*\n\n• Answer questions about your sales, customers and products data\n• Convert plain English to SQL automatically\n• Export results as CSV\n• Generate charts for date range queries\n\nJust type `/ask-data <your question>` to get started!"
                    }
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "💡 Show Suggestions"},
                            "action_id": "show_welcome",
                            "style": "primary"
                        }
                    ]
                }
            ]
        })

# ── Category Button Handlers ──────────────────────────────────────────────────
@app.action("suggest_sales")
def handle_suggest_sales(ack, respond):
    ack()
    respond(build_suggestions_message("sales"))

@app.action("suggest_customers")
def handle_suggest_customers(ack, respond):
    ack()
    respond(build_suggestions_message("customers"))

@app.action("suggest_products")
def handle_suggest_products(ack, respond):
    ack()
    respond(build_suggestions_message("products"))

@app.action("show_welcome")
def handle_show_welcome(ack, respond):
    ack()
    respond(build_welcome_message())

# ── Quick Query Button Handlers ───────────────────────────────────────────────
@app.action("quick_revenue_region")
def handle_quick_revenue(ack, respond, body):
    ack()
    user_id = body["user"]["id"]
    user_name = body["user"]["username"]
    channel_id = body["channel"]["id"]
    execute_query("show total revenue by region ordered by highest",
                  user_id, user_name, channel_id, respond)

@app.action("quick_top_customers")
def handle_quick_customers(ack, respond, body):
    ack()
    user_id = body["user"]["id"]
    user_name = body["user"]["username"]
    channel_id = body["channel"]["id"]
    execute_query("show top 5 customers by total spent",
                  user_id, user_name, channel_id, respond)

@app.action("quick_low_stock")
def handle_quick_stock(ack, respond, body):
    ack()
    user_id = body["user"]["id"]
    user_name = body["user"]["username"]
    channel_id = body["channel"]["id"]
    execute_query("show products with stock quantity less than 100",
                  user_id, user_name, channel_id, respond)

# ── CSV Export ────────────────────────────────────────────────────────────────
@app.action("export_csv")
def handle_export_csv(ack, body, respond):
    ack()
    columns = last_query_data.get("columns", [])
    rows = last_query_data.get("rows", [])
    if not columns or not rows:
        respond("No query data available to export.")
        return
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    writer.writerows(rows)
    csv_bytes = io.BytesIO(buf.getvalue().encode())
    channel_id = body["channel"]["id"]
    client.files_upload_v2(
        channel=channel_id,
        file=csv_bytes,
        filename="query_results.csv",
        title="📥 Query Results CSV"
    )

# ── Flask Setup ───────────────────────────────────────────────────────────────
flask_app = Flask(__name__)
handler = SlackRequestHandler(app)

@flask_app.route("/slack/events", methods=["POST"])
def slack_events():
    return handler.handle(request)

if __name__ == "__main__":
    print("⚡ Slack AI Data Bot running — with welcome messages & suggestions!")
    flask_app.run(port=3000)
