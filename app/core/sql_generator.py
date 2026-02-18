# FILE 2: sql_generator.py
# Purpose: Generate SQL from natural language using Llama 3.2 and handle ambiguity.
# Dependencies: ollama, python-dotenv, re

import os
import re
import ollama
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_LLM_MODEL = os.getenv("OLLAMA_LLM_MODEL", "llama3.2")

# Initialize Ollama client
ollama_client = ollama.Client(host=OLLAMA_BASE_URL)

class SQLGenerator:
    """
    Handles generation of SQL queries from natural language questions using Llama 3.2.
    Includes logic for ambiguity detection and response parsing.
    """

    def __init__(self):
        self.model = OLLAMA_LLM_MODEL

    def handle_ambiguity(self, question: str) -> Optional[str]:
        """
        Checks if the question is too ambiguous to generate valid SQL.
        Returns a clarification request string if ambiguous, else None.
        """
        # Patterns that indicate vague requests without specific metrics
        ambiguous_patterns = [
            r"top customers\??$",  # e.g., "Show me top customers" (by what? spend? orders?)
            r"best products\??$",  # e.g., "Who are the best products"
            r"recent orders\??$",  # e.g., "Show recent orders" (how many?)
            r"performance\??$",    # e.g., "Show performance"
        ]

        for pattern in ambiguous_patterns:
            if re.search(pattern, question, re.IGNORECASE):
                return (
                    f"Your question '{question}' is a bit ambiguous. "
                    "Could you specify the metric? For example: "
                    "'Top 5 customers by total spend' or 'Recent 10 orders'."
                )
        return None

    def generate_sql(self, question: str, schema_context: List[str]) -> str:
        """
        Generates a SQL query for the given question using the provided schema context.
        """
        schema_text = "\n".join(schema_context)
        
        system_prompt = (
            "You are an expert SQL generator for PostgreSQL.\n"
            "Hard rules:\n"
            "1. ONLY generate SELECT statements.\n"
            "2. ONLY use tables/columns from the provided schema.\n"
            "3. NO invented columns.\n"
            "4. NO subqueries unless absolutely necessary.\n"
            "5. Always use table aliases (e.g., FROM customers c).\n"
            "6. Always qualify column names with alias (e.g., c.email).\n"
            "7. Add LIMIT clause when user asks for 'top N' or ranking.\n"
            "8. Do not write any explanation. Do not use markdown. Start your response directly with SELECT.\n\n"
            "Schema:\n"
            f"{schema_text}\n"
        )

        user_prompt = (
            "Few-shot examples:\n"
            "Q: Which 5 customers spent the most?\n"
            "A: SELECT c.name, SUM(o.total_amount) as total_spent FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.name ORDER BY total_spent DESC LIMIT 5;\n\n"
            "Q: What is the total revenue by product category this year?\n"
            "A: SELECT p.category, SUM(oi.unit_price * oi.quantity) as revenue FROM products p JOIN order_items oi ON p.product_id = oi.product_id JOIN orders o ON oi.order_id = o.order_id WHERE o.order_date >= '2024-01-01' GROUP BY p.category;\n\n"
            "Q: How many orders are pending?\n"
            "A: SELECT COUNT(o.order_id) FROM orders o WHERE o.status = 'Pending';\n\n"
            f"Question: {question}\n"
            "SQL Query:"
        )

        try:
            response = ollama_client.chat(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                options={"temperature": 0}
            )
            return response["message"]["content"]
        except Exception as e:
            return f"Error calling Ollama: {str(e)}"

    def parse_sql_from_response(self, response: str) -> str:
        """
        Cleans and extracts just the SQL query from the LLM response.
        """
        # 1. Strip markdown code fences
        clean = re.sub(r"```(sql)?", "", response, flags=re.IGNORECASE).strip()
        
        # 2. Extract starting from SELECT
        # Find first occurrence of SELECT (case-insensitive)
        select_match = re.search(r"\bSELECT\b", clean, re.IGNORECASE)
        if select_match:
            clean = clean[select_match.start():]
        else:
            # If no SELECT found, it might be an error or prose, return as is (validator will catch it)
            return clean

        # 3. Strip trailing prose
        # Look for the last semicolon
        last_semicolon = clean.rfind(";")
        if last_semicolon != -1:
            clean = clean[:last_semicolon+1]
        
        # 4. Normalize whitespace
        clean = " ".join(clean.split())
        
        return clean
