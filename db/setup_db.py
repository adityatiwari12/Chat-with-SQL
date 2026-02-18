import os
import sys
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path to import seed_data
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load env
env_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "chatdb")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")

def get_connection():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def run_sql_file(conn, file_path):
    print(f"Running SQL file: {file_path}")
    with open(file_path, 'r') as f:
        sql = f.read()
    
    with conn.cursor() as cur:
        # Split by simple ; for basic statements? 
        # Actually create_tables.sql has BEGIN/COMMIT so it should run as one block ideally
        # But psycopg2 can handle multi-statement strings usually.
        cur.execute(sql)
    print("SQL execution complete.")

def main():
    print("Starting Database Setup...")
    conn = get_connection()
    
    # 1. Run Create Tables
    create_tables_path = Path(__file__).resolve().parent / 'create_tables.sql'
    if not create_tables_path.exists():
        print(f"Error: {create_tables_path} not found.")
        sys.exit(1)
        
    try:
        run_sql_file(conn, create_tables_path)
    except Exception as e:
        print(f"Error running SQL: {e}")
        sys.exit(1)
    finally:
        conn.close()

    # 2. Run Seed Data
    print("Running Seed Data Script...")
    try:
        # Import here to avoid running it on module load if it has global code
        # But our seed_data has if __name__ == "__main__".
        # Better to run it as a subprocess to ensure clean state
        import subprocess
        seed_script = Path(__file__).resolve().parent / 'seed_data.py'
        subprocess.run([sys.executable, str(seed_script), "--force"], check=True)
    except Exception as e:
        print(f"Error running seed script: {e}")
        sys.exit(1)

    print("\n✅ Database Setup Complete!")

if __name__ == "__main__":
    main()
