import sys
import os

# Ensure the current directory is in python path
sys.path.append(os.getcwd())

try:
    print("Verifying imports...")
    from schema_indexer import SchemaIndexer
    from sql_generator import SQLGenerator
    from sql_validator import SQLValidator
    from db_executor import DatabaseExecutor
    from answer_generator import AnswerGenerator
    from pipeline import ChatWithSQLPipeline
    
    print("Instantiating logic...")
    pipeline = ChatWithSQLPipeline()
    
    print("Checking pydantic models...")
    from api import AskRequest, SchemaIndexRequest
    
    print("SUCCESS: All components imported and instantiated.")
except ImportError as e:
    print(f"FAILURE: Import error: {e}")
except Exception as e:
    print(f"FAILURE: Runtime error: {e}")
