# 🤖 Slack AI Data Bot

An AI-powered Slack bot that lets you query your database using natural language. Ask questions in plain English and get instant SQL results, charts, and CSV exports — right inside Slack.

## ✨ Features

- **Natural Language to SQL** — Converts plain English questions into PostgreSQL queries using LLM (Groq + LLaMA 3.3)
- **Interactive UI** — Welcome messages, category suggestions, and quick-start buttons
- **Auto-Retry** — Automatically retries failed queries with corrected SQL (up to 3 attempts)
- **CSV Export** — Export query results as CSV files directly in Slack
- **Chart Generation** — Automatically generates bar charts for date-range queries
- **SQL Validation** — Blocks dangerous queries (DROP, DELETE, INSERT, etc.)
- **Access Control** — Restrict bot usage to authorized Slack user IDs
- **Query Logging** — Logs all queries to the database for audit trails

## 📊 Available Tables

| Table | Description |
|-------|-------------|
| `sales_daily` | Daily sales data by region and category |
| `customers` | Customer details, spending, and signup info |
| `products` | Product inventory, pricing, and suppliers |

## 🛠️ Tech Stack

- **Python** + Flask
- **Slack Bolt** for Python
- **LangChain** + Groq (LLaMA 3.3 70B)
- **PostgreSQL** (via psycopg2)
- **Matplotlib** + Pandas for charts

## 🚀 Setup

### 1. Clone the repo

```bash
git clone https://github.com/shashank41105/slack-ai-data-bot.git
cd slack-ai-data-bot
```

### 2. Create a virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
# Edit .env with your actual credentials
```

### 5. Run the bot

```bash
python app.py
```

The bot will start on port 3000. Use [ngrok](https://ngrok.com/) to expose it for Slack event subscriptions.

## 💬 Usage

- **Slash Command**: `/ask-data <your question>`
- **DM the bot**: Say `hi` or `hello` for the interactive menu
- **Quick queries**: Use the suggestion buttons for common queries

### Example Questions

- *"Show total revenue by region"*
- *"Who are the top 5 customers by spending?"*
- *"Which products have stock less than 100?"*
- *"Show revenue between 2025-09-01 and 2025-09-02"*

## 📄 License

MIT
