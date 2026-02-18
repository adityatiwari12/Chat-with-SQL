# FILE 1: schema_indexer.py
# Purpose: Index database schema into ChromaDB using Ollama embeddings and search for relevant tables.
# Dependencies: chromadb, ollama, pydantic, python-dotenv

import os
import chromadb
from chromadb import Documents, EmbeddingFunction, Embeddings
import ollama
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")
CHROMA_PERSIST_PATH = os.getenv("CHROMA_PERSIST_PATH", "./chroma_db")

# Initialize Ollama client
ollama_client = ollama.Client(host=OLLAMA_BASE_URL)

class OllamaEmbeddingFunction(EmbeddingFunction):
    """Custom embedding function using Ollama's nomic-embed-text model."""
    
    def __init__(self, client: ollama.Client):
        self.client = client

    def __call__(self, input: Documents) -> Embeddings:
        embeddings = []
        for text in input:
            response = self.client.embeddings(
                model=OLLAMA_EMBED_MODEL,
                prompt=text
            )
            embeddings.append(response["embedding"])
        return embeddings

class SchemaIndexer:
    """
    Handles indexing of database schema metadata into ChromaDB and retrieval of relevant tables
    for a given user question.
    """

    def __init__(self, persist_path: str = CHROMA_PERSIST_PATH):
        self.chroma_client = chromadb.PersistentClient(path=persist_path)
        self.embedding_function = OllamaEmbeddingFunction(client=ollama_client)
        self.collection = self.chroma_client.get_or_create_collection(
            name="schema_metadata",
            embedding_function=self.embedding_function
        )
        self.cached_schema_metadata: Dict[str, Dict] = {}

    def index_schema(self, schema_metadata: List[Dict[str, Any]]):
        """
        Indexes the provided schema metadata into ChromaDB.
        
        Args:
            schema_metadata: List of dicts, each containing 'table_name', 'description', 
                             'columns', 'primary_keys', 'foreign_keys'.
        """
        ids = []
        documents = []
        metadatas = []

        print(f"Indexing {len(schema_metadata)} tables...")

        for table in schema_metadata:
            table_name = table["table_name"]
            description = table.get("description", "")
            
            # Cache for quick lookup later
            self.cached_schema_metadata[table_name] = table

            # Format columns
            columns_str_list = []
            for col in table["columns"]:
                col_str = f"{col['name']} ({col['type']}"
                if col['name'] in table.get("primary_keys", []):
                    col_str += ", PK"
                
                # Check for FK
                is_fk = False
                for fk in table.get("foreign_keys", []):
                    if col['name'] == fk['column']:
                        col_str += f", FK→{fk['references_table']}"
                        is_fk = True
                        break
                
                col_str += ")"
                columns_str_list.append(col_str)
            
            columns_desc = ", ".join(columns_str_list)

            # Format relationships
            relationships = []
            for fk in table.get("foreign_keys", []):
                relationships.append(f"{fk['column']} references {fk['references_table']}.{fk['references_column']}")
            relationships_str = "; ".join(relationships) if relationships else "None"

            # Create rich text document
            document = (
                f"Table: {table_name} | "
                f"Description: {description} | "
                f"Columns: {columns_desc} | "
                f"Relationships: {relationships_str}"
            )

            ids.append(table_name)
            documents.append(document)
            metadatas.append({"table_name": table_name})

        # Upsert into ChromaDB
        if ids:
            self.collection.upsert(
                ids=ids,
                documents=documents,
                metadatas=metadatas
            )
        print("Indexing complete.")

    def retrieve_relevant_tables(self, question: str, top_k: int = 5) -> List[str]:
        """
        Retrieves the top_k most relevant table schemas for a given question.
        Returns a list of formatted table schema strings.
        """
        # Embed the question using the same embedding function implicitly handled by Chroma query if we passed it in create?
        # Actually Chroma's query method automatically uses the embedding function if provided during get_or_create_collection.
        # So we just pass list of texts.
        
        results = self.collection.query(
            query_texts=[question],
            n_results=top_k
        )
        
        # results['documents'] is a list of lists (one list per query)
        retrieved_schemas = results['documents'][0] if results['documents'] else []
        return retrieved_schemas

    def expand_with_related_tables(self, retrieved_tables: List[str]) -> List[str]:
        """
        Parses the retrieved table schemas to find tables involved in Foreign Key relationships
        that were not originally retrieved, and adds them to the list.
        """
        # Basic set of known tables
        known_tables = set()
        for schema in retrieved_tables:
            if schema.startswith("Table: "):
                t_name = schema.split("|")[0].replace("Table: ", "").strip()
                known_tables.add(t_name)
        
        tables_to_add = set()

        # Check for FK references in the retrieved tables
        for schema in retrieved_tables:
            # Look for "FK→target_table" in the text
            parts = schema.split("|")
            for part in parts:
                if "Columns:" in part:
                    token_parts = part.split(",")
                    for token in token_parts:
                        if "FK→" in token:
                            # Extract table name: "... FK→customers)"
                            start_idx = token.find("FK→") + 3
                            # It might end with ) or just be the word
                            candidate = token[start_idx:]
                            if ")" in candidate:
                                candidate = candidate.split(")")[0]
                            ref_table = candidate.strip()
                            
                            if ref_table and ref_table not in known_tables:
                                tables_to_add.add(ref_table)

        # Retrieve documents for the related tables
        if tables_to_add:
            # We need to fetch these from Chroma by ID (table name)
            # get() method: ids=...
            print(f"Expanding context with related tables: {tables_to_add}")
            extras = self.collection.get(ids=list(tables_to_add))
            if extras and extras['documents']:
                for doc in extras['documents']:
                    # Double check we don't duplicate
                    if doc not in retrieved_tables:
                        retrieved_tables.append(doc)
                        
                        # Add to known tables to suppress further duplicates if we recursed (we won't here)
                        if doc.startswith("Table: "):
                            t_name = doc.split("|")[0].replace("Table: ", "").strip()
                            known_tables.add(t_name)
        
        return retrieved_tables

def run_indexing():
    """Run indexing for the example schema."""
    indexer = SchemaIndexer()
    
    # 5 tables example schema
    schema = [
        {
            "table_name": "customers",
            "description": "Stores information about customers including their contact details and creation date.",
            "columns": [
                {"name": "customer_id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "VARCHAR", "nullable": False},
                {"name": "email", "type": "VARCHAR", "nullable": False},
                {"name": "country", "type": "VARCHAR", "nullable": True},
                {"name": "created_at", "type": "TIMESTAMP", "nullable": False}
            ],
            "primary_keys": ["customer_id"],
            "foreign_keys": []
        },
        {
            "table_name": "orders",
            "description": "Stores customer orders including date, status, and total amount.",
            "columns": [
                {"name": "order_id", "type": "INTEGER", "nullable": False},
                {"name": "customer_id", "type": "INTEGER", "nullable": False},
                {"name": "order_date", "type": "DATE", "nullable": False},
                {"name": "status", "type": "VARCHAR", "nullable": False},
                {"name": "total_amount", "type": "DECIMAL", "nullable": False}
            ],
            "primary_keys": ["order_id"],
            "foreign_keys": [
                {"column": "customer_id", "references_table": "customers", "references_column": "customer_id"}
            ]
        },
        {
            "table_name": "products",
            "description": "Stores product catalog details including price and stock.",
            "columns": [
                {"name": "product_id", "type": "INTEGER", "nullable": False},
                {"name": "product_name", "type": "VARCHAR", "nullable": False},
                {"name": "category", "type": "VARCHAR", "nullable": False},
                {"name": "price", "type": "DECIMAL", "nullable": False},
                {"name": "stock_quantity", "type": "INTEGER", "nullable": False}
            ],
            "primary_keys": ["product_id"],
            "foreign_keys": []
        },
        {
            "table_name": "order_items",
            "description": "Stores individual items within an order, linking orders to products.",
            "columns": [
                {"name": "item_id", "type": "INTEGER", "nullable": False},
                {"name": "order_id", "type": "INTEGER", "nullable": False},
                {"name": "product_id", "type": "INTEGER", "nullable": False},
                {"name": "quantity", "type": "INTEGER", "nullable": False},
                {"name": "unit_price", "type": "DECIMAL", "nullable": False}
            ],
            "primary_keys": ["item_id"],
            "foreign_keys": [
                {"column": "order_id", "references_table": "orders", "references_column": "order_id"},
                {"column": "product_id", "references_table": "products", "references_column": "product_id"}
            ]
        },
        {
            "table_name": "payments",
            "description": "Stores payment transactions for orders.",
            "columns": [
                {"name": "payment_id", "type": "INTEGER", "nullable": False},
                {"name": "order_id", "type": "INTEGER", "nullable": False},
                {"name": "payment_date", "type": "DATE", "nullable": False},
                {"name": "amount", "type": "DECIMAL", "nullable": False},
                {"name": "method", "type": "VARCHAR", "nullable": False}
            ],
            "primary_keys": ["payment_id"],
            "foreign_keys": [
                {"column": "order_id", "references_table": "orders", "references_column": "order_id"}
            ]
        }
    ]

    indexer.index_schema(schema)

if __name__ == "__main__":
    run_indexing()
