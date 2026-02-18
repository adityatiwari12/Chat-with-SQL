# FILE 7: api.py
# Purpose: FastAPI application exposing the system.
# Dependencies: fastapi, uvicorn, pydantic, pipeline

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from app.core.pipeline import ChatWithSQLPipeline, PipelineResult

app = FastAPI(title="Chat with SQL API", description="RAG-based Text-to-SQL System", version="1.0.0")

# Input Models
class AskRequest(BaseModel):
    question: str
    session_id: Optional[str] = None

class SchemaIndexRequest(BaseModel):
    schema_metadata: List[Dict[str, Any]]

# Initialize Pipeline
pipeline = ChatWithSQLPipeline()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    # Perform health check
    try:
        pipeline.check_ollama_health()
        print("Initial health check passed.")
    except Exception as e:
        print(f"Startup Warning: {e}")

@app.post("/ask", response_model=PipelineResult)
async def ask_question(request: AskRequest):
    if len(request.question) > 500:
        raise HTTPException(status_code=400, detail="Question too long (max 500 chars).")
    
    try:
        result = await pipeline.process_question(request.question)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/index-schema")
async def index_schema(request: SchemaIndexRequest):
    try:
        pipeline.indexer.index_schema(request.schema_metadata)
        return {"status": "indexed", "table_count": len(request.schema_metadata)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    # Check Ollama
    ollama_status = False
    models = []
    try:
        # We need to access logic from pipeline or do it here
        # Doing it here for separate validation
        import ollama
        import os
        client = ollama.Client(host=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"))
        resp = client.list()
        models = [m['name'] for m in resp['models']]
        ollama_status = True
    except:
        ollama_status = False

    # Check DB
    # We can try a simple query via executor
    db_status = False
    try:
        # Assuming executor _get_connection works
        conn = pipeline.executor._get_connection()
        conn.close()
        db_status = True
    except:
        db_status = False
        
    return {
        "status": "ok" if (ollama_status and db_status) else "degraded",
        "ollama": ollama_status,
        "models_available": models,
        "chroma": True, # basic assumption if file system works
        "db": db_status
    }

@app.get("/schema-preview")
async def schema_preview():
    # Return list of indexed tables if possible.
    # SchemaIndexer doesn't expose a list method directly on collection easily without query.
    # But we cached it in memory in the indexer class instance (ephemeral per worker).
    # If app restarts, cache is gone, but Chroma persists.
    # We can query Chroma for all *metadatas*.
    try:
        count = pipeline.indexer.collection.count()
        if count == 0:
            return {"tables": []}
        
        # Get first 100 items (assuming schema isn't huge)
        data = pipeline.indexer.collection.get(limit=100, include=["metadatas", "documents"])
        tables = []
        if data and data['documents']:
            for doc, meta in zip(data['documents'], data['metadatas']):
                # meta is dict, 'table_name'
                tables.append({
                    "table_name": meta.get("table_name", "unknown"),
                    "preview": doc[:100] + "..."
                })
        return {"tables": tables}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
