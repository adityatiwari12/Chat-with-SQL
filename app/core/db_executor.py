# FILE 4: db_executor.py
# Purpose: Execute SQL on PostgreSQL and handle retries with feedback.
# Dependencies: psycopg2, ollama, pydantic, python-dotenv

import os
import psycopg2
import time
import ollama
from typing import List, Optional, Any, Tuple
from pydantic import BaseModel
from dotenv import load_dotenv

from pathlib import Path
env_path = Path(__file__).resolve().parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
print(f"Loading .env from: {env_path}")
print(f"DB Config: {os.getenv('POSTGRES_DB')}")

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
SQL_TIMEOUT_SECONDS = int(os.getenv("SQL_TIMEOUT_SECONDS", "30"))
MAX_ROWS = int(os.getenv("MAX_ROWS", "200"))
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_LLM_MODEL = os.getenv("OLLAMA_LLM_MODEL", "llama3.2")

ollama_client = ollama.Client(host=OLLAMA_BASE_URL)

class QueryResult(BaseModel):
    success: bool
    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    truncated: bool
    error_message: Optional[str] = None
    execution_time_ms: float

class DatabaseExecutor:
    """
    Executes SQL queries against PostgreSQL with safeguards and retry logic.
    """

    def _get_connection(self):
        return psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

    def execute_query(self, sql: str) -> QueryResult:
        """
        Executes a SQL query and returns formatted results.
        """
        start_time = time.time()
        conn = None
        try:
            conn = self._get_connection()
            # Debug connection
            print(f"Connected to DB: {POSTGRES_DB} User: {POSTGRES_USER} Host: {POSTGRES_HOST}")
            
            # Set read-only transaction
            conn.set_session(readonly=True) # or separate SQL command
            
            with conn.cursor() as cur:
                # Set timeout
                cur.execute(f"SET statement_timeout = {SQL_TIMEOUT_SECONDS * 1000};")
                
                # Execute query
                cur.execute(sql)
                
                # Fetch columns
                columns = [desc[0] for desc in cur.description] if cur.description else []
                
                # Fetch rows (limited)
                rows = cur.fetchmany(MAX_ROWS)
                
                truncated = False
                # Check if there are more rows
                # Note: fetchmany moves cursor. If we want to know if there are more, we can try fetching one more?
                # But fetchmany(200) just returns up to 200.
                # To really know if truncated without fetching all, we'd need LIMIT 201.
                # But the generator adds LIMIT. 
                # Let's just assume valid behavior for now.
                if len(rows) == MAX_ROWS:
                    truncated = True # Potentially truncated

                execution_time = (time.time() - start_time) * 1000
                
                return QueryResult(
                    success=True,
                    columns=columns,
                    rows=[list(row) for row in rows],
                    row_count=len(rows),
                    truncated=truncated,
                    execution_time_ms=execution_time
                )

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            return QueryResult(
                success=False,
                columns=[],
                rows=[],
                row_count=0,
                truncated=False,
                error_message=str(e),
                execution_time_ms=execution_time
            )
        finally:
            if conn:
                conn.close()

    def retry_with_feedback(self, sql: str, error: str, question: str, schema_context: List[str]) -> str:
        """
        Uses Ollama to correct the SQL query based on the error message.
        """
        schema_text = "\n".join(schema_context)
        
        prompt = (
            f"The following SQL failed with error: {error}\n"
            f"Original question: {question}\n"
            f"Available schema: {schema_text}\n"
            f"Failed SQL: {sql}\n"
            "Return ONLY the corrected SQL query, nothing else. Do not use markdown."
        )

        try:
            response = ollama_client.chat(
                model=OLLAMA_LLM_MODEL,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                options={"temperature": 0}
            )
            return response["message"]["content"]
        except Exception as e:
            # If retry fails, return original or failure
            return f"-- Retry failed: {str(e)}"

    def format_rows_as_text(self, result: QueryResult) -> str:
        """
        Formats query results as a markdown table for the LLM to read.
        Caps at 50 rows to avoid token overflow.
        """
        if not result.success:
            return f"Error: {result.error_message}"
        
        if not result.rows:
            return "No results found."

        # Limit to 50 rows for prompt context
        display_rows = result.rows[:50]
        
        # Calculate column widths
        col_widths = [len(c) for c in result.columns]
        for row in display_rows:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))
        
        # Create header
        header = "| " + " | ".join(f"{c:<{w}}" for c, w in zip(result.columns, col_widths)) + " |"
        separator = "| " + " | ".join("-" * w for w in col_widths) + " |"
        
        lines = [header, separator]
        
        for row in display_rows:
            line = "| " + " | ".join(f"{str(val):<{w}}" for val, w in zip(row, col_widths)) + " |"
            lines.append(line)
            
        if len(result.rows) > 50:
            lines.append(f"... ({len(result.rows) - 50} more rows truncated)")
            
        return "\n".join(lines)
