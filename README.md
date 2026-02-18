# Chat with SQL System

A complete RAG-based Text-to-SQL system that allows you to ask natural language questions about your database and get conversational answers.

## 🏗️ Architecture

```mermaid
graph TD
    User[User] -->|Natural Language Question| API[FastAPI Endpoint]
    API --> Pipeline[RAG Pipeline]
    Pipeline -->|Retrieve Schema| ChromaDB[ChromaDB (Vector Store)]
    Pipeline -->|Context + Question| LLM[Ollama (Llama 3.2)]
    LLM -->|Generate SQL| Validator[SQL Validator]
    Validator -->|Execute SQL| DB[PostgreSQL Database]
    DB -->|Results| LLM
    LLM -->|Natural Language Answer| User
```

## 📖 Documentation

For a deeper understanding of the system, please refer to:
- [**Architecture Guide**](ARCHITECTURE.md): Technical deep-dive and data flow.
- [**Project Improvements**](PROJECT_IMPROVEMENTS.md): Road-map and identified technical enhancements.

## 📂 Project Structure

```text
Chat with SQL/
├── app/                        # Main application package
│   ├── api/                    # API endpoints and entry point
│   │   └── main.py             # FastAPI app
│   └── core/                   # Core logic modules
│       ├── pipeline.py         # Orchestration logic
│       ├── schema_indexer.py   # RAG Indexer
│       ├── sql_generator.py    # SQL Generation
│       ├── sql_validator.py    # Safety Checks
│       ├── db_executor.py      # Database Execution
│       └── answer_generator.py # Answer Synthesis
├── database/                   # Database management scripts
│   ├── create_tables.sql       # DDL Schema
│   ├── seed_data.py            # Data Seeding
│   └── verify_data.py          # Data Verification
├── tests/                      # Testing
│   └── test_setup.py           # Setup verification
├── requirements.txt            # Dependencies
├── .env.example                # Configuration template
├── PROJECT_STATUS.md           # Current status report
└── ARCHITECTURE.md             # Detailed architecture docs
```

## 🚀 Getting Started

### Prerequisites

1.  **Ollama**: [Download](https://ollama.ai) and install.
    -   Run: `ollama serve`
    -   Pull models: `ollama pull llama3.2` and `ollama pull nomic-embed-text`
2.  **PostgreSQL**: Installed and running.
3.  **Python 3.10+**

### Setup

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure Environment**:
    -   Copy `.env.example` to `.env`
    -   Update DB credentials in `.env`

3.  **Setup Database**:
    ```bash
    createdb chatdb
    psql -d chatdb -f database/create_tables.sql
    python database/seed_data.py
    ```

4.  **Index Schema**:
    ```bash
    python -m app.core.schema_indexer
    ```
    *(Note: You might need to adjust python path or run a script that imports it correctly if direct module execution has issues with relative imports. Alternatively, use the API endpoint `/index-schema`)*

5.  **Run the Application**:

    You need two terminals:

    **Terminal 1 (Backend API):**
    ```bash
    uvicorn app.api.main:app --reload
    ```
    *API will be available at http://localhost:8000*

    **Terminal 2 (Frontend UI):**
    ```bash
    streamlit run app/ui/streamlit_app.py
    ```
    *UI will open at http://localhost:8501*

## 🧪 Usage

**Ask a Question**:
```bash
curl -X POST "http://localhost:8000/ask" \
     -H "Content-Type: application/json" \
     -d '{"question": "Who are the top 5 customers by spending?"}'
```

---

## 🛠️ Components

-   **Schema Indexer**: Embeds table metadata (columns, relationships) into ChromaDB.
-   **SQL Generator**: Uses Llama 3.2 to write SQL. Includes specific rules and few-shot prompting.
-   **SQL Validator**: Using `sqlparse` to validte structure and block harmful commands.
-   **DB Executor**: Runs queries with `READ ONLY` transaction mode and retry logic.
-   **Answer Generator**: Synthesizes the final response.
