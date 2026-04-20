from fastapi import FastAPI, HTTPException, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import json
from typing import Dict, Any, Optional
import os
import re as r
import logging
from pathlib import Path
from fastapi.middleware.cors import CORSMiddleware

from src.config import Config
from src.embeddingmodel import EmbeddingGenerator
from src.vectordb import QdrantStore
from src.structuraldb import DB
from src.logger import setup_logging, set_session_id, clear_session_id

# Setup JSON structured logging
setup_logging(service_name="api", level=Config.LOG_LEVEL)
logger = logging.getLogger(__name__)

# Initialize FastAPI
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

import uuid as _uuid
from starlette.middleware.base import BaseHTTPMiddleware

class _RequestIdMiddleware(BaseHTTPMiddleware):
    """Injects request ID into log context for every HTTP request."""
    async def dispatch(self, request, call_next):
        req_id = request.headers.get("X-Request-ID") or str(_uuid.uuid4())[:8]
        token = set_session_id(req_id)
        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = req_id
            return response
        finally:
            clear_session_id(token)

app.add_middleware(_RequestIdMiddleware)

# Dependencies
def get_db():
    db = DB()
    try:
        yield db
    finally:
        db.close()

def get_embedding_generator():
    return EmbeddingGenerator(api_key=Config.GEMINI_APIKEY)

def get_vector_store(embed_gen: EmbeddingGenerator = Depends(get_embedding_generator)):
    return QdrantStore(embedding_model=embed_gen.embeddings)

# Helper functions
def extract_solution(llm_solution_string: str, solution_id: str) -> str:
    try:
        if not llm_solution_string:
            return ""
        # Convert JSON string to dict
        data = json.loads(llm_solution_string)
        # Build key dynamically → solution1 / solution2 / solution3
        key = f"solution{solution_id}"
        # Return only instructions text
        return data.get(key, {}).get("instructions", "")
    except Exception as e:
        logger.warning(f"Failed to extract solution: {e}")
        return ""

def clean_error_description(text: str) -> dict:
    """Normalize/clean incoming description """
    if not text:
        return {"cleanText": ""}

    s = text
    s = s.replace("\\n", " ").replace("\n", " ")
    s = s.replace('\\"', "").replace("&quot;", "")
    s = r.sub(r"\s+", " ", s)
    s = r.sub(r"[\(\)\[\]\{\}]", "", s)
    s = r.sub(r"[\'\":;^|\-,]", "", s)
    s = s.strip().strip('"').strip("'")
    return {"cleanText": s}

def get_ui_template(template_name: str) -> str:
    """Load HTML template from UI directory"""
    base_dir = Path(__file__).resolve().parents[1]
    template_path = base_dir / "UI" / template_name
    
    if not template_path.exists():
        logger.error(f"Template not found: {template_path}")
        raise HTTPException(status_code=500, detail="UI Template not found")
        
    return template_path.read_text(encoding="utf-8")

# Routes
@app.get("/health")
async def health_check():
    from time import time
    return {"status": "healthy", "timestamp": time()}

@app.get("/exposeFeedBackUI-for-solutionSelection", response_class=HTMLResponse)
async def load_solution(request: Request, db: DB = Depends(get_db)):
    token = request.query_params.get("token")
    solution_id = request.query_params.get("SOLUTION_ID")
    
    if not token or not solution_id:
        return HTMLResponse("Missing token or SOLUTION_ID", status_code=400)
    
    rows = db.execute(
        "SELECT * FROM errorsolutiontable WHERE sessionid=%s",
        (token,), 
        fetch=True
    )
    
    if not rows:
        return HTMLResponse("Session not found", status_code=404)
        
    record = rows[0]
    
    payload = {
        "serviceName": record.get("application_name"),
        "environment": "Non Prod", # TODO: Make dynamic
        "timestamp": record.get("error_timestamp"),
        "errorType": record.get("error_code"),
        "errorMessage": record.get("error_description"),
        "errorId": record.get("id"),
        "sessionId": record.get("sessionid"),
        "solution": record.get("llm_solution"),
    }
    
    selected_solution = extract_solution(payload.get("solution"), solution_id)
    
    html_template = get_ui_template("llm-suggested-submit-ui.html")
    
    html = (
        html_template
        .replace("{{ERROR_ID}}", str(payload.get("errorId")))
        .replace("{{ERROR_TYPE}}", str(payload.get("errorType")))
        .replace("{{SOLUTION_TEXT}}", selected_solution)
        .replace("{{SOLUTION_ID}}", str(solution_id))
    )
    return HTMLResponse(html, media_type="text/html")

@app.get("/exposeFeedBackUI-for-CustomSolution", response_class=HTMLResponse)
async def custom_solution(request: Request, db: DB = Depends(get_db)):
    token = request.query_params.get("token")
    
    if not token:
        return HTMLResponse("Missing token", status_code=400)

    rows = db.execute(
        "SELECT * FROM errorsolutiontable WHERE sessionid=%s",
        (token,), 
        fetch=True
    )
    
    if not rows:
        return HTMLResponse("Session not found", status_code=404)

    record = rows[0]
    payload = {
        "serviceName": record.get("application_name"),
        "environment": "Non Prod",
        "timestamp": record.get("error_timestamp"),
        "errorType": record.get("error_code"),
        "errorMessage": record.get("error_description"),
        "errorId": record.get("id"),
        "sessionId": record.get("sessionid"),
        "solution": record.get("llm_solution"),
    }
    
    html_template = get_ui_template("custom-solution-submit-ui.html")
    
    html = (
        html_template
        .replace("{{ERROR_ID}}", str(payload.get("errorId")))
        .replace("{{ERROR_TYPE}}", str(payload.get("errorType")))
    )
    return HTMLResponse(html, media_type="text/html")

@app.post("/submitopssolution")
async def update_vector(
    request: Request, 
    db: DB = Depends(get_db),
    # Injected dependencies for better testing/performance
    embed_gen: EmbeddingGenerator = Depends(get_embedding_generator),
    store: QdrantStore = Depends(get_vector_store)
):
    payload = await request.json()
    record_id = payload.get("errorId")
    custom_solution = payload.get("customSolution")
    selected_solution_id = payload.get("solutionId")
    solution_timestamp = payload.get("solutionTimestamp")
    
    # Check if session is active
    rows = db.execute(
        "SELECT sessionid_status FROM errorsolutiontable WHERE id=%s",
        (record_id,), fetch=True
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Record not found")
    
    if rows[0]["sessionid_status"] != "active":
        return JSONResponse(
            content={"error": "Session Expired", "status": "inactive", "message": "Your session has expired"},
            status_code=404
        )
        
    # If custom solution is not provided, fetch from LLM solution
    if custom_solution is None:
        llm_rows = db.execute(
            "SELECT error_code,error_description,llm_solution FROM errorsolutiontable WHERE id=%s",
            (record_id,), fetch=True
        )
        if not llm_rows:
            raise HTTPException(status_code=404, detail="Record not found (deleted between checks)")
        row = llm_rows[0]

        llm_solution_dict = json.loads(row["llm_solution"])
        solution_key = f"solution{selected_solution_id}"
        custom_solution = llm_solution_dict.get(solution_key, {}).get("instructions")

    # Update DB
    db.execute(
        """UPDATE errorsolutiontable
           SET ops_solution=%s, ops_solution_timestamp=%s, sessionid_status=%s
           WHERE id=%s""",
        (custom_solution, solution_timestamp, "inactive", record_id,)
    )
    
    # Fetch record again for embedding creation
    # Optimization: We might already have this info if we fetched it above, 
    # but strictly speaking we only fetched it if custom_solution was None.
    # Safe to fetch again to be sure we have latest state.
    record = db.execute(
        "SELECT application_name,error_code,error_description,ops_solution FROM errorsolutiontable WHERE id=%s",
        (record_id,), fetch=True
    )[0]
    
    clean_desc = clean_error_description(record["error_description"])
    embed_input = f"Error:{record['error_code']} Description:{clean_desc.get('cleanText', '')}"
    
    # Generate Embedding
    raw_embedding_input = embed_gen.get_embedding(embed_input)
    
    # Upsert to Vector DB
    store.upsert_vector(
        collection="error_solutions",
        vector_id=int(record_id),
        vector=raw_embedding_input,
        payload={
            "error_code": record.get("error_code", ""),
            "error_description": record.get("error_description", ""),
            "solution": custom_solution
        }
    )
    
    return {"message": "Vector DB updated successfully", "status": "SUCCESS"}

class SolutionIngestRequest(BaseModel):
    error_code: str
    description: str
    solution: str

@app.post("/ingest-solution")
async def ingest_solution(
    item: SolutionIngestRequest,
    embed_gen: EmbeddingGenerator = Depends(get_embedding_generator),
    store: QdrantStore = Depends(get_vector_store)
):
    """
    Manual endpoint to ingest verified solutions directly into Vector DB.
    """
    try:
        # Generate a unique integer ID from UUID for Qdrant (which expects int/uuid)
        # Using simple integer hash of uuid to fit standard int if needed, 
        # but Qdrant supports UUID strings directly if using models.PointStruct(id="uuid", ...).
        # However, our QdrantStore wrapper implies ID. Let's use a large random int.
        import random
        # 64-bit integer range
        new_id = random.randint(0, 10_000_000_000) 
        
        clean_desc = clean_error_description(item.description)
        embed_input = f"Error:{item.error_code} Description:{clean_desc.get('cleanText', '')}"
        
        # Generate Embedding
        raw_embedding_input = embed_gen.get_embedding(embed_input)
        
        # Upsert
        store.upsert_vector(
            collection="error_solutions",
            vector_id=new_id,
            vector=raw_embedding_input,
            payload={
                "error_code": item.error_code,
                "error_description": item.description,
                "solution": item.solution
            }
        )
        
        logger.info(f"Manually ingested solution for {item.error_code} (ID: {new_id})")
        return {"status": "SUCCESS", "message": "Solution ingested", "id": new_id}
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Dashboard API Endpoints
# ============================================================

@app.get("/getsolutions")
async def get_solutions(
    db: DB = Depends(get_db),
    app_name: Optional[str] = None,
    error_type: Optional[str] = None,
    status: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    search: Optional[str] = None,
    page: int = 1,
    page_size: int = 20
):
    """Return paginated error records with optional filters."""
    conditions = []
    params: list = []

    if app_name:
        conditions.append("application_name = %s")
        params.append(app_name)
    if error_type:
        conditions.append("error_type = %s")
        params.append(error_type.lower())
    if status == "resolved":
        conditions.append("ops_solution IS NOT NULL")
    elif status == "unresolved":
        conditions.append("ops_solution IS NULL")
    if from_date:
        conditions.append("error_timestamp >= %s")
        params.append(from_date)
    if to_date:
        conditions.append("error_timestamp <= %s")
        params.append(to_date)
    if search:
        conditions.append("(error_code ILIKE %s OR error_description ILIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    count_sql = f"SELECT COUNT(*) as total FROM errorsolutiontable {where_clause}"
    count_rows = db.execute(count_sql, tuple(params), fetch=True)
    total = count_rows[0]["total"] if count_rows else 0

    offset = (page - 1) * page_size
    data_sql = f"""
        SELECT id, application_name, error_code, error_type,
               error_description, error_timestamp, occurrence_count,
               sessionid_status,
               CASE WHEN ops_solution IS NULL THEN false ELSE true END as is_resolved,
               ops_solution, llm_solution, sessionid
        FROM errorsolutiontable
        {where_clause}
        ORDER BY error_timestamp DESC
        LIMIT %s OFFSET %s
    """
    rows = db.execute(data_sql, tuple(params) + (page_size, offset), fetch=True)

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
        "records": rows or []
    }


@app.get("/stats/summary")
async def stats_summary(
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
    db: DB = Depends(get_db)
):
    """Overall error counts, optionally filtered by date range."""
    conditions = []
    params: list = []
    if from_date:
        conditions.append("error_timestamp >= %s")
        params.append(from_date)
    if to_date:
        conditions.append("error_timestamp <= %s")
        params.append(to_date)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    rows = db.execute(f"""
        SELECT
            COUNT(*)                                                     AS total,
            COUNT(*) FILTER (WHERE ops_solution IS NOT NULL)             AS resolved,
            COUNT(*) FILTER (WHERE ops_solution IS NULL)                 AS unresolved,
            COUNT(*) FILTER (WHERE error_type = 'technical')             AS technical,
            COUNT(*) FILTER (WHERE error_type = 'business')              AS business,
            COUNT(*) FILTER (WHERE error_type = 'platform')              AS platform,
            COUNT(*) FILTER (WHERE error_timestamp::date = CURRENT_DATE) AS today
        FROM errorsolutiontable
        {where}
    """, tuple(params), fetch=True)

    row = rows[0] if rows else {}
    return {
        "total":      row.get("total", 0),
        "resolved":   row.get("resolved", 0),
        "unresolved": row.get("unresolved", 0),
        "technical":  row.get("technical", 0),
        "business":   row.get("business", 0),
        "platform":   row.get("platform", 0),
        "today":      row.get("today", 0),
    }


@app.get("/stats/by-application")
async def stats_by_application(
    from_date: Optional[str] = None,
    to_date:   Optional[str] = None,
    db: DB = Depends(get_db)
):
    """Error counts grouped by application, optionally filtered by date range."""
    conditions = ["application_name IS NOT NULL"]
    params: list = []
    if from_date:
        conditions.append("error_timestamp >= %s")
        params.append(from_date)
    if to_date:
        conditions.append("error_timestamp <= %s")
        params.append(to_date)
    where = "WHERE " + " AND ".join(conditions)

    rows = db.execute(f"""
        SELECT
            application_name,
            COUNT(*)                                             AS total,
            COUNT(*) FILTER (WHERE ops_solution IS NOT NULL)     AS resolved,
            COUNT(*) FILTER (WHERE ops_solution IS NULL)         AS unresolved
        FROM errorsolutiontable
        {where}
        GROUP BY application_name
        ORDER BY total DESC
        LIMIT 20
    """, tuple(params), fetch=True)
    return {"data": rows or []}


@app.get("/stats/trends")
async def stats_trends(days: int = 7, db: DB = Depends(get_db)):
    """Daily error counts for the last N days for the line chart."""
    if days not in (7, 14, 30):
        days = 7
    rows = db.execute("""
        SELECT
            error_timestamp::date                                    AS day,
            COUNT(*)                                                 AS total,
            COUNT(*) FILTER (WHERE error_type = 'technical')        AS technical,
            COUNT(*) FILTER (WHERE error_type = 'business')         AS business,
            COUNT(*) FILTER (WHERE error_type = 'platform')         AS platform
        FROM errorsolutiontable
        WHERE error_timestamp >= CURRENT_DATE - INTERVAL '%s days'
        GROUP BY error_timestamp::date
        ORDER BY day ASC
    """, (days,), fetch=True)
    return {"days": days, "data": rows or []}


@app.get("/applications")
async def get_applications(db: DB = Depends(get_db)):
    """Distinct application names for the filter dropdown."""
    rows = db.execute("""
        SELECT DISTINCT application_name
        FROM errorsolutiontable
        WHERE application_name IS NOT NULL
        ORDER BY application_name
    """, fetch=True)
    return {"applications": [row["application_name"] for row in rows] if rows else []}
