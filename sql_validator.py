# FILE 3: sql_validator.py
# Purpose: Validate generated SQL for safety and correctness.
# Dependencies: sqlparse, pydantic

import re
import sqlparse
from typing import List, Dict, Optional, Any
from pydantic import BaseModel

class ValidationResult(BaseModel):
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    extracted_tables: List[str]
    extracted_columns: List[str]

class SQLValidator:
    """
    Validates SQL queries to ensure they are safe (read-only) and semantically correct
    against the allowed schema.
    """

    def __init__(self):
        self.forbidden_keywords = {
            "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", 
            "CREATE", "EXEC", "EXECUTE", "GRANT", "REVOKE", "REPLACE"
        }

    def parse_and_validate(self, sql: str, allowed_tables: List[str], allowed_columns: Dict[str, List[str]] = None) -> ValidationResult:
        """
        Parses SQL and validates against allowed schema and safety rules.
        """
        errors = []
        warnings = []
        extracted_tables = []
        extracted_columns = []

        # 1. Basic cleaning and parsing
        parsed = sqlparse.parse(sql)
        if not parsed:
            return ValidationResult(is_valid=False, errors=["Empty SQL query"], warnings=[], extracted_tables=[], extracted_columns=[])
        
        stmt = parsed[0]
        
        # 2. Check first keyword
        if stmt.get_type() != "SELECT":
             errors.append(f"Query must start with SELECT. Found type: {stmt.get_type()}")

        # 3. Scan tokens for forbidden keywords
        # Flatten tokens to search easily
        for token in stmt.flatten():
            if token.ttype in (sqlparse.tokens.Keyword, sqlparse.tokens.DML, sqlparse.tokens.DDL):
                word = token.value.upper()
                if word in self.forbidden_keywords:
                    errors.append(f"Forbidden keyword found: {word}")
                if word == "UNION":
                    warnings.append("UNION usage detected. Ensure column alignment.")

        # 4. Extract tables and validate
        # This is tricky with sqlparse, but we can iterate through tokens
        # A simpler robust way for "tables" is looking for Identifier after FROM/JOIN
        # We will use valid table names from allowed_tables to check existence rather than perfect extraction
        
        # Simple extraction logic: find all words that match allowed tables
        # This acts as a whitelist verification.
        # If a table is used that ISN'T in allowed_tables, we might miss it if we only search for allowed ones.
        # So strict parsing is better.
        
        # Extract tables from statement
        found_tables = self._extract_tables(stmt)
        extracted_tables = list(found_tables)

        for table in found_tables:
            if table not in allowed_tables:
                # Handle aliases? schema_indexer returns full table names.
                # If table matches an alias (e.g. "customers c"), the extractor should grab "customers".
                errors.append(f"Access to table '{table}' is not allowed or not in retrieved context.")

        # 5. Extract columns (simplified)
        # We won't implement strict column validation here as it requires deep parsing of aliases
        # But we can check for suspiciously valid columns if allowed_columns is provided
        # For now, we skip deep column validation to avoid false positives, 
        # relying on the database executor to catch execution errors.

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            extracted_tables=extracted_tables,
            extracted_columns=[]
        )

    def _extract_tables(self, stmt) -> set:
        """
        Extracts table names from a parsed SQL statement.
        """
        tables = set()
        
        def extract_from_token(token):
            if isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    extract_from_token(identifier)
            elif isinstance(token, sqlparse.sql.Identifier):
                # It's a table if it appears in FROM or JOIN
                # But here we just want the real name, not alias
                real_name = token.get_real_name()
                if real_name:
                    tables.add(real_name)
            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "FROM":
                # Next tokens usually contain table
                pass 
                
        # Better approach: iterate and track state "seen FROM/JOIN"
        # But sqlparse has a helper for this? No, not really reliable.
        # Let's use a simpler heuristic for this demo:
        # Scan for FROM/JOIN and take the next Identifier
        
        # Actually sqlparse has .get_real_name() on Identifiers
        # We iterate all Identifiers and check if they look like tables? 
        # No, that's too broad (cols are identifiers too).
        
        # Alternative: use regex for FROM/JOIN table extraction as backup to sqlparse complexity
        # Helper:
        str_sql = str(stmt).upper()
        # Remove subqueries or handle them? 
        # Simple regex for "FROM table" and "JOIN table"
        # Handles "FROM table alias"
        patterns = [
            r"FROM\s+([a-zA-Z0-9_]+)",
            r"JOIN\s+([a-zA-Z0-9_]+)",
            r"UPDATE\s+([a-zA-Z0-9_]+)" # Should not happen but for completeness
        ]
        
        for pat in patterns:
            matches = re.findall(pat, str_sql)
            for m in matches:
                tables.add(m.lower()) # Normalize to lower assumption
        
        return tables

    def sanitize_sql(self, sql: str) -> str:
        """
        Strips comments and normalizes SQL.
        """
        # Remove single line comments
        sql = re.sub(r"--.*", "", sql)
        # Remove multi-line comments
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        # Strip trailing semicolon
        sql = sql.rstrip(";").strip()
        # Normalize whitespace
        sql = " ".join(sql.split())
        return sql

    def check_for_injection(self, sql: str) -> bool:
        """
        Checks for obvious injection patterns like stacked queries.
        Returns True if injection detected.
        """
        # Check for stacked queries (semicolon in middle of string)
        # We stripped trailing semicolon in sanitize_sql, so any remaining semicolon is suspicious
        if ";" in sql:
            return True
        
        # Check for hex encoding (0x...) which is common in exploits but rare in legitimate queries
        if "0x" in sql.lower():
             # Basic heuristic, might produce false positives for literal hex data
             pass 
        
        return False
