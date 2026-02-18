import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time
import subprocess
from dotenv import load_dotenv

# Ensure we can import from app
sys.path.append(os.getcwd())

def run_command(cmd, shell=True):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=shell, check=False)
    if result.returncode != 0:
        print(f"Warning: Command failed with code {result.returncode}")
        return False
    return True

def setup_env():
    if not os.path.exists(".env"):
        print("Creating .env from .env.example...")
        with open(".env.example", "r") as f:
            content = f.read()
        with open(".env", "w") as f:
            f.write(content)
    load_dotenv()

def create_database():
    host = os.getenv("POSTGRES_HOST", "localhost")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "password")
    dbname = os.getenv("POSTGRES_DB", "postgres")
    target_db = "chatdb"

    print(f"Connecting to Postgres at {host} as {user}...")
    try:
        conn = psycopg2.connect(host=host, user=user, password=password, dbname=dbname)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Check if exists
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{target_db}'")
        if not cur.fetchone():
            print(f"Creating database '{target_db}'...")
            cur.execute(f"CREATE DATABASE {target_db}")
        else:
            print(f"Database '{target_db}' already exists.")
        
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error creating database: {e}")
        print("Please ensure PostgreSQL is running and credentials in .env are correct.")
        return False

def run_ddl():
    print("Running DDL...")
    # Using db_executor logic or raw psycopg2
    # We can just read the SQL file and execute
    try:
        from database import seed_data # To reuse connection logic if needed, but lets just connect
        
        # Override DB name for connection
        os.environ["POSTGRES_DB"] = "chatdb"
        
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            dbname="chatdb",
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "password")
        )
        conn.autocommit = True
        
        with open("database/create_tables.sql", "r") as f:
            sql = f.read()
            
        with conn.cursor() as cur:
            # Split by ; to run statement by statement? 
            # Psycopg2 execute() can handle multiple statements usually
            cur.execute(sql)
            
        conn.close()
        print("DDL executed successfully.")
        return True
    except Exception as e:
        print(f"DDL Execution Error: {e}")
        return False

def seed_and_verify():
    print("Seeding data...")
    try:
        # Import as module
        from database import seed_data
        # Force reload env since we modified it? seed_data loads it internally
        seed_data.main()
        
        print("Verifying data...")
        from database import verify_data
        verify_data.main()
        return True
    except Exception as e:
        print(f"Seeding/Verification Error: {e}")
        return False

def index_rag():
    print("Indexing Schema for RAG...")
    try:
        # Run module
        run_command("python -m app.core.schema_indexer")
        return True
    except Exception as e:
        print(f"Indexing Error: {e}")
        return False

if __name__ == "__main__":
    setup_env()
    if create_database():
        if run_ddl():
            if seed_and_verify():
                if index_rag():
                    print("\n\nAll systems GO! You can now start the API.")
