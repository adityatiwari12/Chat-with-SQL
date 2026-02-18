# Chat with SQL System

A complete "Chat with SQL" system that allows users to ask natural language questions about a large relational database and receive natural language answers. The system uses RAG (Retrieval-Augmented Generation) to handle large schemas efficiently and runs entirely locally using Ollama.

## Architecture

```ascii
+-------------+      +----------------+      +------------------+
|    User     | ---> |    FastAPI     | ---> | ChatPipeline     |
+-------------+      +----------------+      +------------------+
                                                      |
                                                      v
                                             +------------------+
                                             |  SchemaIndexer   |
                                             |   (ChromaDB)     |
                                             +------------------+
                                                      |
                                                      v
                                             +------------------+
                                             |   SQLGenerator   |
                                             |   (Llama 3.2)    |
                                             +------------------+
                                                      |
                                                      v
                                             +------------------+
                                             |   SQLValidator   |
                                             +------------------+
                                                      |
                                                      v
                                             +------------------+
                                             | DatabaseExecutor |
                                             |  (PostgreSQL)    |
                                             +------------------+
                                                      |
                                                      v
                                             +------------------+
                                             | AnswerGenerator  |
                                             |   (Llama 3.2)    |
                                             +------------------+
```

## Prerequisites

1.  **Install Ollama**: Download from [https://ollama.ai](https://ollama.ai)
2.  **Pull Models**:
    ```bash
    ollama pull llama3.2
    ollama pull nomic-embed-text
    ```
3.  **Start Ollama**:
    ```bash
    ollama serve
    ```
4.  **PostgreSQL**: Ensure you have a PostgreSQL database running.

## Setup Instructions

1.  **Clone the repository** (if applicable).
2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Configure Environment**:
    - Copy `.env.example` to `.env`
    - Update `POSTGRES_*` credentials in `.env`
4.  **Index Schema**:
    - You can run `python schema_indexer.py` to index the example schema immediately (it contains a `run_indexing()` function).
    - Or use the API: `POST /index-schema`
5.  **Start the API**:
    ```bash
    uvicorn api:app --reload
    ```

## Usage Example

**Ask a Question:**

```bash
curl -X POST http://localhost:8000/ask \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Which 5 customers spent the most money?"
  }'
```

## How RAG Works

1.  **Indexing**: Database tables are converted into rich text documents describing their columns and relationships. These are embedded using `nomic-embed-text` and stored in ChromaDB.
2.  **Retrieval**: When a user asks a question, we embed the question and find the most relevant table schemas.
3.  **Expansion**: We automatically pull in related tables (via Foreign Keys) to ensuring JOINs are possible.
4.  **Generation**: The LLM receives the question + only the relevant schema (not the whole DB) to generate accurate SQL.

## Changing Models

To use different Ollama models, simply update `.env`:

```env
OLLAMA_LLM_MODEL=mistral
OLLAMA_EMBED_MODEL=mxbai-embed-large
```

## Known Limitations

-   **Local LLM Accuracy**: Llama 3.2 is powerful but may occasionally make syntax errors compared to GPT-4. The system includes a retry mechanism with error feedback to mitigate this.
-   **Cold Start**: The first request might be slower as Ollama loads the models into memory.
-   **Complex Queries**: Extremely complex nested queries might still challenge smaller local models.

## Troubleshooting

-   **Ollama Connection Refused**: Ensure `ollama serve` is running and `OLLAMA_BASE_URL` is correct.
-   **Model Not Found**: Run `ollama list` to check if `llama3.2` and `nomic-embed-text` are pulled.
-   **ChromaDB Dimension Mismatch**: If you change embedding models, delete the `./chroma_db` folder to reset the collection.
-   **PostgreSQL Errors**: Check credentials in `.env` and ensuring the database exists.
-   **Prose in SQL**: If the model is chatty, the `SQLValidator` and `parse_sql_from_response` logic attempts to clean it, but stricter prompting (already implemented) helps.
