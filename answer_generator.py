# FILE 5: answer_generator.py
# Purpose: Generate natural language answers from query results.
# Dependencies: ollama, pydantic, python-dotenv

import os
import ollama
from typing import Dict, Any, List
from dotenv import load_dotenv
# We need to import QueryResult from db_executor but to avoid circular imports if any (none here), it's fine.
# But for type hinting clean-ness, we might redefine or import.
from db_executor import QueryResult

load_dotenv()

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_LLM_MODEL = os.getenv("OLLAMA_LLM_MODEL", "llama3.2")

ollama_client = ollama.Client(host=OLLAMA_BASE_URL)

class AnswerGenerator:
    """
    Generates a conversational answer based on the SQL query results.
    """

    def generate_answer(self, question: str, sql: str, result: QueryResult) -> str:
        """
        Synthesizes the natural language answer.
        """
        # 1. Handle errors
        if not result.success:
            return f"I couldn't get the answer because of a database error: {result.error_message}"
        
        # 2. Handle empty results
        if result.row_count == 0:
            return "I ran the query but found no results. You might want to check if the data exists for that specific criteria."

        # 3. Format rows for context
        # We duplicate logic from db_executor format_rows_as_text or allow direct access?
        # db_executor has format_rows_as_text, but it's an instance method.
        # Ideally we should refrain from cross-dependencies if possible or just implement helper here.
        formatted_rows = self._format_rows(result)
        
        prompt = (
            f"Question: {question}\n"
            f"SQL used: {sql}\n"
            f"Results:\n{formatted_rows}\n\n"
            "Provide a 2-4 sentence summary answering the question."
        )

        system_prompt = (
            "You are a helpful data analyst. Summarize query results in clear, conversational language. "
            "Use specific numbers and names from the data. Answer the user's question directly. "
            "Do not add information not present in the data. "
            "Respond in plain text only, no markdown formatting."
        )

        try:
            response = ollama_client.chat(
                model=OLLAMA_LLM_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                options={"temperature": 0.3}
            )
            answer = response["message"]["content"]
            
            if result.truncated:
                answer += " (Note: Results were truncated to 200 rows.)"
            
            return answer
        except Exception as e:
            return f"Error generating answer: {str(e)}"

    def format_for_display(self, answer: str, result: QueryResult, sql: str) -> Dict[str, Any]:
        """
        Formats the final output for the API response.
        """
        return {
            "answer": answer,
            "sql_used": sql,
            "row_count": result.row_count,
            "execution_time_ms": result.execution_time_ms,
            "data_preview": [dict(zip(result.columns, row)) for row in result.rows[:5]]
        }

    def _format_rows(self, result: QueryResult) -> str:
        """Helper to format rows for the prompt."""
        # Simple CSV-like format for LLM is usually efficient enough
        if not result.rows:
            return "No data"
        
        header = ",".join(result.columns)
        rows_str = []
        for row in result.rows[:20]: # Only top 20 for answer context to save tokens
            rows_str.append(",".join(map(str, row)))
        
        return f"{header}\n" + "\n".join(rows_str)
