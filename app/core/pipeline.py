# FILE 6: pipeline.py
# Purpose: Orchestrates the entire RAG -> SQL -> Answer flow.
# Dependencies: all previous modules, pydantic

import time
import ollama
import os
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from dotenv import load_dotenv

from .schema_indexer import SchemaIndexer
from .sql_generator import SQLGenerator
from .sql_validator import SQLValidator
from .db_executor import DatabaseExecutor, QueryResult
from .answer_generator import AnswerGenerator

load_dotenv()
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_LLM_MODEL = os.getenv("OLLAMA_LLM_MODEL", "llama3.2")

ollama_client = ollama.Client(host=OLLAMA_BASE_URL)

class PipelineResult(BaseModel):
    question: str
    generated_sql: Optional[str]
    validation_result: Optional[dict]
    query_result: Optional[dict]
    answer: str
    clarification_needed: bool = False
    total_time_ms: float

class ChatWithSQLPipeline:
    def __init__(self):
        self.indexer = SchemaIndexer()
        self.sql_gen = SQLGenerator()
        self.validator = SQLValidator()
        self.executor = DatabaseExecutor()
        self.answer_gen = AnswerGenerator()

    def check_ollama_health(self):
        """Checks if Ollama is running and models are available."""
        try:
            models = ollama_client.list()
            # models is usually {'models': [{'name': '...', ...}]}
            model_names = [m['name'] for m in models['models']]
            
            has_llm = any("llama3.2" in m for m in model_names)
            has_embed = any("nomic-embed-text" in m for m in model_names)
            
            if not has_llm or not has_embed:
                # Warning only, don't crash, maybe they have different names or tags
                print(f"WARNING: Preferred models not found in {model_names}. Expecting llama3.2 and nomic-embed-text.")
        except Exception as e:
            # If we can't connect, that's critical
            raise RuntimeError(f"Ollama health check failed: {str(e)}. Is Ollama running?")

    async def process_question(self, question: str) -> PipelineResult:
        start_time = time.time()
        
        # Step 1: Check ambiguity
        ambiguity = self.sql_gen.handle_ambiguity(question)
        if ambiguity:
            return PipelineResult(
                question=question,
                generated_sql=None,
                validation_result=None,
                query_result=None,
                answer=ambiguity,
                clarification_needed=True,
                total_time_ms=(time.time() - start_time) * 1000
            )

        # Step 2: Retrieve tables
        retrieved_schemas = self.indexer.retrieve_relevant_tables(question, top_k=5)
        
        # Step 3: Expand tables
        full_context_schemas = self.indexer.expand_with_related_tables(retrieved_schemas)
        
        # Extract table names for validation
        allowed_tables = set()
        for s in full_context_schemas:
            if s.startswith("Table: "):
                t_name = s.split("|")[0].replace("Table: ", "").strip()
                allowed_tables.add(t_name)

        # Step 4: Generate SQL
        raw_sql = self.sql_gen.generate_sql(question, full_context_schemas)
        clean_sql = self.sql_gen.parse_sql_from_response(raw_sql)

        # Step 5: Validate SQL
        clean_sql = self.validator.sanitize_sql(clean_sql)
        validation = self.validator.parse_and_validate(clean_sql, list(allowed_tables))
        
        if not validation.is_valid:
            # Short-circuit if invalid
            return PipelineResult(
                question=question,
                generated_sql=clean_sql,
                validation_result=validation.model_dump(),
                query_result=None,
                answer=f"I generated SQL that didn't pass validation: {'; '.join(validation.errors)}",
                total_time_ms=(time.time() - start_time) * 1000
            )

        # Step 6: Execute SQL with retry
        query_result = self.executor.execute_query(clean_sql)
        
        # Retry logic
        if not query_result.success:
            print(f"Query failed: {query_result.error_message}. Retrying...")
            corrected_sql = self.executor.retry_with_feedback(
                clean_sql, 
                query_result.error_message, 
                question, 
                full_context_schemas
            )
            corrected_sql = self.sql_gen.parse_sql_from_response(corrected_sql)
            corrected_sql = self.validator.sanitize_sql(corrected_sql)
            
            validation_retry = self.validator.parse_and_validate(corrected_sql, list(allowed_tables))
            
            if validation_retry.is_valid:
                clean_sql = corrected_sql
                validation = validation_retry
                query_result = self.executor.execute_query(corrected_sql)
                # If still fails, query_result.success is False, handled below

        # Step 7: Generate Answer
        final_answer = self.answer_gen.generate_answer(question, clean_sql, query_result)
        
        return PipelineResult(
            question=question,
            generated_sql=clean_sql,
            validation_result=validation.model_dump(),
            query_result=self._serialize_query_result(query_result),
            answer=final_answer,
            total_time_ms=(time.time() - start_time) * 1000
        )

    def _serialize_query_result(self, qr: QueryResult) -> dict:
        return {
            "success": qr.success,
            "row_count": qr.row_count,
            "rows": qr.rows[:5], # Only show preview
            "columns": qr.columns,
            "error_message": qr.error_message,
            "execution_time_ms": qr.execution_time_ms
        }

    def explain_query(self, question: str, sql: str) -> str:
        """Helper to explain SQL in plain English."""
        prompt = (
            f"Explain what this SQL query does in simple English for a non-technical user.\n"
            f"Question: {question}\n"
            f"SQL: {sql}\n"
            "Keep it to 1-2 sentences."
        )
        try:
            response = ollama_client.chat(
                model=OLLAMA_LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                options={"temperature": 0.3}
            )
            return response["message"]["content"]
        except:
            return "Could not generate explanation."
